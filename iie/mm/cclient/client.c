#include "mmcc.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <netinet/in.h> 
#include "hiredis.h"
#include <netdb.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <limits.h>
#include "hvfs_u.h"
#include "xlist.h"
#include "xlock.h"
#include "memory.h"
#include "xhash.h"

//编译时要加上-l hiredis

#define HVFS_TRACING
#include "tracing.h"

HVFS_TRACING_INIT();

unsigned int hvfs_mmcc_tracing_flags = HVFS_DEFAULT_LEVEL;

#define SYNCSTORE       1
#define SEARCH          2
#define DELSET          3
#define ASYNCSTORE      4
#define SERVERINFO      5
#define XSEARCH         9

char *g_uris = NULL;
char *redisHost = NULL;
int redisPort = -1;
long g_ckpt_ts = -1;
long g_ssid = -1;

struct MMSConf
{
    int dupnum;
#define MMSCONF_DEDUP   0x01
#define MMSCONF_NODUP   0x02
#define MMSCONF_DUPSET  0x04
    int mode;
    int sockperserver;
    int logdupinfo;
    int redistimeout;
    __timer_cb tcb;
};

struct MMSCSock
{
    struct list_head list;
    int sock;
#define SOCK_FREE               0
#define SOCK_INUSE              1
    int state;
};

struct MMSConnection
{
    long sid;
    char *hostname;

    struct hlist_node hlist;
    
    xlock_t lock;
    struct list_head socks;
    int sock_nr;
    int port;
    time_t ttl;                 /* update ttl to detect removal */
    char *sp;                   /* server:port info from active.mms */
};

struct redisConnection
{
    time_t born;                /* born time */
    time_t ttl;                 /* this connection used last time */
    pid_t tid;
    struct hlist_node hlist;
    atomic_t ref;

    redisContext *rc;
    xlock_t lock;
};

struct MMSConf g_conf = {
    .dupnum = 2,
    .mode = MMSCONF_DEDUP,
    .sockperserver = 5,
    .logdupinfo = 1,
    .redistimeout = 60,
    .tcb = NULL,
};

/* quick lookup from THREADID to redisContext */
static struct regular_hash *g_rcrh = NULL;
static int g_rcrh_size = 1024;

/* quick lookup from SID to MMSConnection */
static struct regular_hash *g_rh = NULL;
static int g_rh_size = 32;
static int g_sid_max = 0;

static sem_t g_timer_sem;
static pthread_t g_timer_thread = 0;
static int g_timer_thread_stop = 0;
static int g_timer_interval = 30;
static int g_clean_rcs = 3600;

time_t g_client_tick = 0;

atomic_t g_master_connect_err = {.counter = 0,};

void __clean_rcs(time_t cur);
void do_update();
void __master_connect_fail_check();

void mmcc_debug_mode(int enable)
{
    if (enable)
        hvfs_mmcc_tracing_flags = 0xffffffff;
    else
        hvfs_mmcc_tracing_flags = HVFS_DEFAULT_LEVEL;
}

static void *__timer_thread_main(void *arg)
{
    sigset_t set;
    time_t cur;
    static time_t last_check = 0;
    int err;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the timer events */
    while (!g_timer_thread_stop) {
        err = sem_wait(&g_timer_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(mmcc, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        cur = time(NULL);
        if (cur >= last_check + 60) {
            __clean_rcs(cur);
            last_check = cur;
        }

        do_update();

        /* trigger callbacks */
        if (g_conf.tcb) {
            g_conf.tcb(&cur);
        }
    }

    hvfs_debug(mmcc, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

static void __itimer_default(int signo, siginfo_t *info, void *arg)
{
    sem_post(&g_timer_sem);
    g_client_tick = time(NULL);

    return;
}

static int setup_timers(int interval)
{
    struct sigaction ac;
    struct itimerval value, ovalue, pvalue;
    int which = ITIMER_REAL;
    int err;

    sem_init(&g_timer_sem, 0, 0);

    err = pthread_create(&g_timer_thread, NULL, &__timer_thread_main,
                         NULL);
    if (err) {
        hvfs_err(mmcc, "Create timer thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

    memset(&ac, 0, sizeof(ac));
    sigemptyset(&ac.sa_mask);
    ac.sa_flags = 0;
    ac.sa_sigaction = __itimer_default;
    err = sigaction(SIGALRM, &ac, NULL);
    if (err) {
        err = -errno;
        goto out;
    }
    err = getitimer(which, &pvalue);
    if (err) {
        err = -errno;
        goto out;
    }

    if (!interval) interval = 1; /* trigger it every seconds */
    if (interval > 0) {
        value.it_interval.tv_sec = interval;
        value.it_interval.tv_usec = 0;
        value.it_value.tv_sec = interval;
        value.it_value.tv_usec = 0;
        err = setitimer(which, &value, &ovalue);
        if (err) {
            err = -errno;
            goto out;
        }
        hvfs_debug(mmcc, "OK, we have created a timer thread to "
                   " do scans every %d second(s).\n", interval);
    } else {
        hvfs_debug(mmcc, "Hoo, there is no need to setup itimers based on the"
                   " configration.\n");
        g_timer_thread_stop = 1;
    }
    
out:
    return err;
}

struct __client_r_op
{
    char *opname;
    char *script;
    char *sha;
};

#define __CLIENT_R_OP_DUP_DETECT        0

static struct __client_r_op g_ops[] = {
    {
        "dup_detect",
        "local x = redis.call('hexists', KEYS[1], ARGV[1]); if x == 1 then redis.call('hincrby', '_DUPSET_', KEYS[1]..'@'..ARGV[1], 1); return redis.call('hget', KEYS[1], ARGV[1]); else return nil; end",
    },
};

int client_config(mmcc_config_t *mc)
{
    int err = 0;

    if (mc->tcb)
        g_conf.tcb = mc->tcb;
    if (mc->ti > 0)
        g_timer_interval = mc->ti;
    if (mc->rcc > 0)
        g_clean_rcs = mc->rcc;
    if (mc->mode > 0)
        g_conf.mode = mc->mode;

    return err;
}

int client_init()
{
    int err = 0, i;

    g_rh = xzalloc(sizeof(*g_rh) * g_rh_size);
    if (!g_rh) {
        hvfs_err(mmcc, "xzalloc() hash table failed, no memory.\n");
        err = EMMNOMEM;
        goto out;
    }

    for (i = 0; i < g_rh_size; i++) {
        xlock_init(&g_rh[i].lock);
        INIT_HLIST_HEAD(&g_rh[i].h);
    }

    g_rcrh = xzalloc(sizeof(*g_rcrh) * g_rcrh_size);
    if (!g_rcrh) {
        hvfs_err(mmcc, "xzalloc() hash table failed, no memory.\n");
        err = EMMNOMEM;
        goto out_free;
    }

    for (i = 0; i < g_rcrh_size; i++) {
        xlock_init(&g_rcrh[i].lock);
        INIT_HLIST_HEAD(&g_rcrh[i].h);
    }

    err = setup_timers(g_timer_interval);
    if (err) {
        hvfs_err(mmcc, "setup_timers() failed w/ %d\n", err);
        goto out_free2;
    }

out:
    return err;
out_free2:
    xfree(g_rcrh);
out_free:
    xfree(g_rh);
    goto out;
}

struct redisConnection *__alloc_rc(pid_t tid)
{
    struct redisConnection *rc = xzalloc(sizeof(*rc));

    if (rc) {
        rc->born = time(NULL);
        rc->ttl = rc->born;
        rc->tid = tid;
        atomic_set(&rc->ref, 0);
        xlock_init(&rc->lock);
        INIT_HLIST_NODE(&rc->hlist);
    }

    return rc;
}

void __free_rc(struct redisConnection *rc)
{
    xfree(rc);
}

static inline void __get_rc(struct redisConnection *rc)
{
    atomic_inc(&rc->ref);
}

static inline void __put_rc(struct redisConnection *rc)
{
    if (atomic_dec_return(&rc->ref) < 0) {
        __free_rc(rc);
    }
}

void putRC(struct redisConnection *rc)
{
    __put_rc(rc);
}

struct redisConnection *getRC();

static inline void freeRC(struct redisConnection *rc)
{
    if (rc->rc) {
        redisFree(rc->rc);
        rc->rc = NULL;
    }
}

int __clean_up_socks(struct MMSConnection *c);

int __client_load_scripts(struct redisConnection *rc)
{
    redisReply *rpy = NULL;
    int err = 0, i;

    for (i = 0; i < sizeof(g_ops) / sizeof(struct __client_r_op); i++) {
        rpy = redisCommand(rc->rc, "script load %s", g_ops[i].script);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = -EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "script %d load failed w/ \n%s.\n", i, rpy->str);
            err = -EINVAL;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_info(mmcc, "Script %d %s \tloaded as '%s'.\n",
                      i, g_ops[i].opname, rpy->str);
            g_ops[i].sha = strdup(rpy->str);
        } else {
            g_ops[i].sha = NULL;
        }
    out_free:
        freeReplyObject(rpy);
    }

out:
    return err;
}

int client_fina()
{
    /* free active redis connections */
    struct redisConnection *tpos;
    struct MMSConnection *t2pos;
    struct hlist_node *pos, *n;
    struct regular_hash *rh;
    int i;

    /* wait for pending threads */
    g_timer_thread_stop = 1;
    sem_post(&g_timer_sem);
    pthread_join(g_timer_thread, NULL);
    
    for (i = 0; i < g_rcrh_size; i++) {
        rh = g_rcrh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry_safe(tpos, pos, n, &rh->h, hlist) {
            hlist_del_init(&tpos->hlist);
            hvfs_debug(mmcc, "FINA clean active RC for TID %ld\n",
                       (long)tpos->tid);
            freeRC(tpos);
            __put_rc(tpos);
        }
        xlock_unlock(&rh->lock);
    }
    xfree(g_rcrh);

    /* free active MMS connections */
    for (i = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry_safe(t2pos, pos, n, &rh->h, hlist) {
            hlist_del_init(&t2pos->hlist);
            hvfs_debug(mmcc, "FINA clean active MMSC for Server %s:%d\n",
                       t2pos->hostname, t2pos->port);
            __clean_up_socks(t2pos);
            xfree(t2pos->hostname);
            xfree(t2pos->sp);
            xfree(t2pos);
        }
        xlock_unlock(&rh->lock);
    }
    xfree(g_rh);

    /* free any other global resources, and reset them */
    xfree(g_uris);
    g_uris = NULL;
    xfree(redisHost);
    redisHost = NULL;

    return 0;
}

void __clean_rcs(time_t cur)
{
    struct redisConnection *tpos;
    struct hlist_node *pos, *n;
    struct regular_hash *rh;
    int i;

    for (i = 0; i < g_rcrh_size; i++) {
        rh = g_rcrh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry_safe(tpos, pos, n, &rh->h, hlist) {
            hvfs_debug(mmcc, "check TID %ld %ld %ld %d\n", (long)tpos->tid,
                       (tpos->ttl + g_clean_rcs), cur, atomic_read(&tpos->ref));
            if ((tpos->ttl + g_clean_rcs < cur) && atomic_read(&tpos->ref) == 0) {
                hlist_del_init(&tpos->hlist);
                hvfs_debug(mmcc, "Clean long living RC for TID %ld\n", 
                           (long)tpos->tid);
                freeRC(tpos);
                __put_rc(tpos);
            }
        }
        xlock_unlock(&rh->lock);
    }
}

struct redisConnection *__rc_lookup(pid_t tid)
{
    int idx = tid % g_rcrh_size;
    int found = 0;
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct redisConnection *rc;

    rh = g_rcrh + idx;
    xlock_lock(&rh->lock);
    hlist_for_each_entry(rc, pos, &rh->h, hlist) {
        if (tid == rc->tid) {
            /* ok, found it */
            found = 1;
            atomic_inc(&rc->ref);
            break;
        }
    }
    xlock_unlock(&rh->lock);
    if (!found)
        rc = NULL;

    return rc;
}

/*
 * 0 => inserted
 * 1 => not inserted
 */
int __rc_insert(struct redisConnection *rc)
{
    int idx = rc->tid % g_rcrh_size;
    int found = 0;
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct redisConnection *n;

    rh = g_rcrh + idx;
    xlock_lock(&rh->lock);
    hlist_for_each_entry(n, pos, &rh->h, hlist) {
        if (rc->tid == n->tid) {
            found = 1;
            break;
        }
    }
    if (!found) {
        /* ok, insert it to hash table */
        hlist_add_head(&rc->hlist, &rh->h);
    }
    xlock_unlock(&rh->lock);

    return found;
}

void __rc_remove(struct redisConnection *rc)
{
    int idx = rc->tid % g_rcrh_size;
    struct regular_hash *rh;

    rh = g_rcrh + idx;
    xlock_lock(&rh->lock);
    hlist_del_init(&rc->hlist);
    xlock_unlock(&rh->lock);
}

struct MMSConnection *__alloc_mmsc()
{
    struct MMSConnection *c = xzalloc(sizeof(*c));

    if (c) {
        xlock_init(&c->lock);
        INIT_LIST_HEAD(&c->socks);
        INIT_HLIST_NODE(&c->hlist);
    }

    return c;
}

struct MMSConnection *__mmsc_lookup(long sid)
{
    int idx = sid % g_rh_size;
    int found = 0;
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *c;

    rh = g_rh + idx;
    xlock_lock(&rh->lock);
    hlist_for_each_entry(c, pos, &rh->h, hlist) {
        if (sid == c->sid) {
            /* ok, found it */
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);
    if (!found) {
        c = NULL;
    }

    return c;
}

struct MMSConnection *__mmsc_lookup_byname(char *host, int port)
{
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *c;
    int found = 0, i;
    
    for (i = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(c, pos, &rh->h, hlist) {
            if (port == c->port && strcmp(host, c->hostname) == 0) {
                /* ok, found it */
                found = 1;
                break;
            }
        }
        xlock_unlock(&rh->lock);
        if (found)
            break;
    }
    if (!found)
        c = NULL;

    return c;
}


/*
 *  0 => inserted
 *  1 => not inserted
 */
int __mmsc_insert(struct MMSConnection *c)
{
    int idx = c->sid % g_rh_size;
    int found = 0;
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *n;

    if (c->sid > g_sid_max)
        g_sid_max = c->sid;

    rh = g_rh + idx;
    xlock_lock(&rh->lock);
    hlist_for_each_entry(n, pos, &rh->h, hlist) {
        if (c->sid == n->sid) {
            found = 1;
            break;
        }
    }
    if (!found) {
        /* ok, insert it to hash table */
        hlist_add_head(&c->hlist, &rh->h);
    }
    xlock_unlock(&rh->lock);

    return found;
}

void __mmsc_remove(struct MMSConnection *c)
{
    int idx = c->sid % g_rh_size;
    struct regular_hash *rh;

    rh = g_rh + idx;
    xlock_lock(&rh->lock);
    hlist_del_init(&c->hlist);
    xlock_unlock(&rh->lock);
}

void __add_to_mmsc(struct MMSConnection *c, struct MMSCSock *s)
{
    xlock_lock(&c->lock);
    list_add(&s->list, &c->socks);
    c->sock_nr++;
    xlock_unlock(&c->lock);
}

int __clean_up_socks(struct MMSConnection *c)
{
    struct MMSCSock *pos, *n;
    int err = 0;
    
    xlock_lock(&c->lock);
    list_for_each_entry_safe(pos, n, &c->socks, list) {
        if (pos->state == SOCK_INUSE) {
            err = 1;
            break;
        } else {
            list_del(&pos->list);
            close(pos->sock);
            c->sock_nr--;
            xfree(pos);
        }
    }
    xlock_unlock(&c->lock);

    return err;
}

struct MMSCSock *__alloc_mmscsock()
{
    struct MMSCSock *s = xzalloc(sizeof(*s));

    if (s) {
        INIT_LIST_HEAD(&s->list);
    }

    return s;
}

void __free_mmscsock(struct MMSCSock *s) 
{
    xfree(s);
}

void __del_from_mmsc(struct MMSConnection *c, struct MMSCSock *s)
{
    xlock_lock(&c->lock);
    list_del(&s->list);
    c->sock_nr--;
    xlock_unlock(&c->lock);
}

void __put_inuse_sock(struct MMSConnection *c, struct MMSCSock *s)
{
    xlock_lock(&c->lock);
    s->state = SOCK_FREE;
    xlock_unlock(&c->lock);
}

#define redisCMD(rc, a...) ({                    \
            struct redisReply *__rr = NULL;      \
            __rr = redisCommand(rc, ## a);       \
            __rr;                                \
        })

int update_mmserver(struct redisConnection *rc)
{
    redisReply *rpy = NULL, *_rr = NULL;
    int err = 0;

    rpy = redisCMD(rc->rc, "zrange mm.active 0 -1 withscores");
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ARRAY) {
        int i = 0, j;

        /* alloc server list */
        struct MMSConnection *c;

        for (j = 0; j < rpy->elements; j += 2) {
            char *p = rpy->element[j]->str, *n = NULL, *hostname;
            char *q = strdup(rpy->element[j]->str);
            int port;
            long sid;

            /* try to parse hostname to ip address */
            {
                char cmd[256];

                sprintf(cmd, "hget mm.dns %s", p);
                _rr = redisCMD(rc->rc, cmd);
                if (_rr == NULL) {
                    hvfs_warning(mmcc, "try to do dns for '%s' failed %s\n",
                                 p, rc->rc->errstr);
                } else if (_rr->type == REDIS_REPLY_STRING) {
                    hvfs_info(mmcc, "do DNS for %s -> %s\n", p, _rr->str);
                    p = _rr->str;
                }
            }
            
            p = strtok_r(p, ":", &n);
            if (p) {
                hostname = strdup(p);
            } else {
                hvfs_err(mmcc, "strtok_r for MMServer failed, ignore.\n");
                xfree(q);
                if (_rr) freeReplyObject(_rr);
                continue;
            }
            p = strtok_r(NULL, ":", &n);
            if (p) {
                port = atoi(p);
            } else {
                hvfs_err(mmcc, "strtok_r for MMServer port failed, ignore.\n");
                xfree(hostname);
                xfree(q);
                if (_rr) freeReplyObject(_rr);
                continue;
            }
            sid = atol(rpy->element[j + 1]->str);
            
            c = __mmsc_lookup(sid);
            if (!c) {
                c = __alloc_mmsc();
                if (!c) {
                    hvfs_err(mmcc, "alloc MMSC failed, ignore %s:%d\n",
                             hostname, port);
                    xfree(hostname);
                    xfree(q);
                    if (_rr) freeReplyObject(_rr);
                    continue;
                }
                xlock_lock(&c->lock);
                c->ttl = time(NULL);
                xlock_unlock(&c->lock);
            }

            xlock_lock(&c->lock);
            xfree(c->hostname);
            xfree(c->sp);
            c->hostname = hostname;
            c->port = port;
            c->sid = sid;
            c->sp = q;
            xlock_unlock(&c->lock);
            
            hvfs_info(mmcc, "Update MMServer ID=%ld %s:%d\n", c->sid,
                      hostname, port);
            __mmsc_insert(c);
            i++;
            if (_rr) {
                freeReplyObject(_rr);
                _rr = NULL;
            }
        }
        hvfs_info(mmcc, "Got %d MMServer from Meta Server.\n", i);
    }

    freeReplyObject(rpy);
out:
    return err;
}

void update_mmserver2(struct redisConnection *rc)
{
    struct MMSConnection *c;
    redisReply *rpy = NULL;
    int i;

    for (i = 0; i <= g_sid_max; i++) {
        c = __mmsc_lookup(i);
        if (c) {
            rpy = redisCMD(rc->rc, "exists mm.hb.%s",
                           c->sp);
            if (rpy == NULL) {
                hvfs_err(mmcc, "invalid redis status, connection broken?\n");
                redisFree(rc->rc);
                rc->rc = NULL;
                break;
            }
            if (rpy->type == REDIS_REPLY_INTEGER) {
                if (rpy->integer == 1) {
                    c->ttl = time(NULL);
                    hvfs_debug(mmcc, "Update MMServer %s ttl to %ld\n",
                               c->sp, (u64)c->ttl);
                } else {
                    if (time(NULL) - c->ttl > 600)
                        c->ttl = 0;
                    hvfs_warning(mmcc, "MMServer %s ttl lost, do NOT update (ttl=%ld).\n",
                                 c->sp, c->ttl);
                }
            }
            freeReplyObject(rpy);
        }
    }
}

void update_g_info(struct redisConnection *rc)
{
    redisReply *rpy = NULL;

    /* Step 1: get ssid */
    rpy = redisCMD(rc->rc, "get mm.ss.id");
    if (rpy == NULL) {
        hvfs_err(mmcc, "invalid redis status, connection broken?\n");
        redisFree(rc->rc);
        rc->rc = NULL;
        return;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        long _ssid = atol(rpy->str);

        if (_ssid != g_ssid) {
            hvfs_info(mmcc, "MM SS Server change from %ld to %ld.\n",
                      g_ssid, _ssid);
            g_ssid = _ssid;
        }
    }
    freeReplyObject(rpy);

    /* Step 2: get ckpt_ts */
    rpy = redisCMD(rc->rc, "get mm.ckpt.ts");
    if (rpy == NULL) {
        hvfs_err(mmcc, "invalid redis status, connection broken?\n");
        redisFree(rc->rc);
        rc->rc = NULL;
        return;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        long _ckpt = atol(rpy->str);

        if (_ckpt > g_ckpt_ts) {
            hvfs_info(mmcc, "MM CKPT TS change from %ld to %ld\n",
                      g_ckpt_ts, _ckpt);
            g_ckpt_ts = _ckpt;
        } else if (_ckpt < g_ckpt_ts) {
            hvfs_warning(mmcc, "Detect MM CKPT TS change backwards"
                         "(cur %ld, got %ld).\n",
                         g_ckpt_ts, _ckpt);
        }
    }
    freeReplyObject(rpy);
}

void do_update()
{
    struct redisConnection *rc = NULL;
    static time_t last_check = 0;
    static time_t last_fetch = 0;
    time_t cur;

    rc = getRC();
    if (!rc) return;

    cur = time(NULL);
    if (cur >= last_check + 30) {
        update_mmserver2(rc);
        last_check = cur;
    }

    if (cur >= last_fetch + 10) {
        update_g_info(rc);
        last_fetch = cur;
    }

    putRC(rc);
}

int do_connect(struct MMSConnection *c)
{
    struct MMSCSock *s = NULL;
	int sock = socket(AF_INET, SOCK_STREAM, 0);
    int err = 0;

	if (sock == -1) {
		hvfs_err(mmcc, "failed to create a socket!\n");
		return EMMCONNERR;
	}
    s = __alloc_mmscsock();
    if (!s) {
		hvfs_err(mmcc, "failed to create a MMSCSock!\n");
		return EMMNOMEM;
    }
    
	struct hostent *hptr = gethostbyname(c->hostname);

	if (hptr == NULL) {
        hvfs_err(mmcc, "resolve nodename:%s failed!\n", c->hostname);
        err = EMMMETAERR;
        goto out_free;
	}

	struct sockaddr_in dest_addr;

	dest_addr.sin_family = AF_INET;
	dest_addr.sin_port = htons(c->port);
	dest_addr.sin_addr.s_addr = ((struct in_addr *)(hptr->h_addr))->s_addr;
	bzero(&(dest_addr.sin_zero), 8);
    
	int cr = connect(sock, (struct sockaddr*)&dest_addr, sizeof(struct sockaddr));	

	if (cr == -1) {
        hvfs_err(mmcc, "connect to server %s:%d failed\n", c->hostname, c->port);
        err = EMMCONNERR;
        goto out_free;
	}
	
    s->sock = sock;
    s->state = SOCK_FREE;
    __add_to_mmsc(c, s);
    hvfs_info(mmcc, "Create new connection for Server ID=%ld.\n",
              c->sid);

    return 0;
out_free:
    __free_mmscsock(s);
    return err;
}

struct redisConnection *getRC()
{
    pid_t tid = syscall(SYS_gettid);
    struct timeval tv = {
        .tv_sec = g_conf.redistimeout,
        .tv_usec = 0,
    };
    int err = 0;

    /* check if this is a new thread */
    struct redisConnection *rc = __rc_lookup(tid);
    
    if (!rc) {
        rc = __alloc_rc(tid);
        if (!rc) {
            hvfs_err(mmcc, "alloc new RC for thread %ld failed\n", (long)tid);
            err = EMMNOMEM;
            goto out;
        }
        rc->rc = redisConnect(redisHost, redisPort);
        if (rc->rc->err) {
            hvfs_err(mmcc, "can't connect to redis master at %s:%d %s\n", 
                     redisHost, redisPort, rc->rc->errstr);
            redisFree(rc->rc);
            rc->rc = NULL;
            err = EMMMETAERR;
            __put_rc(rc);
            __master_connect_fail_check();
            goto out;
        }
        err = redisSetTimeout(rc->rc, tv);
        if (err) {
            hvfs_err(mmcc, "set redis timeout to %d seconds failed w/ %d\n",
                     (int)tv.tv_sec, err);
        }
        atomic_set(&g_master_connect_err, 0);
        err = update_mmserver(rc);
        if (err) {
            hvfs_err(mmcc, "update_mmserver() failed w/ %d\n", err);
            redisFree(rc->rc);
            __put_rc(rc);
            goto out;
        }
        __get_rc(rc);
        switch (__rc_insert(rc)) {
        case 0:
            /* ok */
            hvfs_info(mmcc, "Create RC for thread %ld\n", (long)tid);
            break;
        default:
        case 1:
            /* somebody insert our TID? */
            hvfs_err(mmcc, "Guy, are you kidding me? TID=%ld\n", (long)tid);
            redisFree(rc->rc);
            __put_rc(rc);
            __put_rc(rc);
            goto out;
            break;
        }
    } else {
        if (unlikely(!rc->rc)) {
            /* reconnect to redis server */
        reconnect:
            rc->rc = redisConnect(redisHost, redisPort);
            if (rc->rc->err) {
                hvfs_err(mmcc, "can't connect to redis master at %s:%d %s\n",
                         redisHost, redisPort, rc->rc->errstr);
                redisFree(rc->rc);
                rc->rc = NULL;
                __put_rc(rc);
                err = EMMMETAERR;
                __master_connect_fail_check();
                goto out;
            }
            err = redisSetTimeout(rc->rc, tv);
            if (err) {
                hvfs_err(mmcc, "set redis timeout to %d seconds failed w/ %d\n",
                         (int)tv.tv_sec, err);
            }
            /* update server array */
            err = update_mmserver(rc);
            if (err) {
                __put_rc(rc);
                hvfs_err(mmcc, "update_mmserver() failed w/ %d\n", err);
                goto out;
            }
            atomic_set(&g_master_connect_err, 0);
        } else {
            /* if ttl larger than 10min, re-test it */
            if (time(NULL) - rc->ttl >= 600) {
                redisReply *rpy = NULL;

                rpy = redisCommand(rc->rc, "ping");
                if (rpy == NULL) {
                    hvfs_err(mmcc, "ping %s:%d failed.\n", redisHost, redisPort);
                    redisFree(rc->rc);
                    rc->rc = NULL;
                    goto reconnect;
                }
                freeReplyObject(rpy);
            }
        }
        rc->ttl = time(NULL);
    }

out:
    if (err)
        rc = NULL;
    
    return rc;
}

struct MMSCSock *__get_free_sock(struct MMSConnection *c)
{
    struct MMSCSock *pos = NULL;
    int found = 0;
    int retry = 0;

    do {
        if (retry >= 10) {
            pos = NULL;
            break;
        }

        xlock_lock(&c->lock);
        list_for_each_entry(pos, &c->socks, list) {
            if (pos->state == SOCK_FREE) {
                found = 1;
                pos->state = SOCK_INUSE;
                break;
            }
        }
        xlock_unlock(&c->lock);
        
        if (!found) {
            if (c->sock_nr < g_conf.sockperserver) {
                /* new connection */
                pos = __alloc_mmscsock();

                if (pos) {
                    int sock = socket(AF_INET, SOCK_STREAM, 0);
                    int port;
                    char *hostname;
                    struct hostent *hptr;
                    struct sockaddr_in dest_addr;
                    int err = 0;

                    if (sock < 0) {
                        int n = 2;

                        hvfs_err(mmcc, "create socket failed w/ %s(%d) (retry=%d)\n",
                                 strerror(errno), errno, retry);
                        __free_mmscsock(pos);
                        do {
                            n = sleep(n);
                        } while (n);
                        retry++;
                        continue;
                    }

                    {
                        int i = 1;
                        err = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, 
                                         (void *)&i, sizeof(i));
                        if (err) {
                            hvfs_warning(mmcc, "setsockopt TCP_NODELAY failed w/ %s\n",
                                         strerror(errno));
                        }
                    }

                    xlock_lock(&c->lock);
                    hostname = strdup(c->hostname);
                    port = c->port;
                    xlock_unlock(&c->lock);
                    
                    hptr = gethostbyname(hostname);
                    if (hptr == NULL) {
                        hvfs_err(mmcc, "resolve nodename:%s failed! (retry=%d)\n", 
                                 hostname, retry);
                        __free_mmscsock(pos);
                        sleep(1);
                        retry++;
                        xfree(hostname);
                        continue;
                    }
                    xfree(hostname);

                    dest_addr.sin_family = AF_INET;
                    dest_addr.sin_port = htons(port);
                    dest_addr.sin_addr.s_addr = ((struct in_addr *)
                                                 (hptr->h_addr))->s_addr;
                    bzero(&(dest_addr.sin_zero), 8);

                    err = connect(sock, (struct sockaddr *)&dest_addr, 
                                  sizeof(struct sockaddr));
                    if (err) {
                        hvfs_err(mmcc, "Connection to server %ld failed w/ %s(%d) "
                                 "(retry=%d).\n",
                                 c->sid, strerror(errno), errno, retry);
                        __free_mmscsock(pos);
                        sleep(1);
                        retry++;
                        continue;
                    }
                    pos->sock = sock;
                    pos->state = SOCK_INUSE;
                    __add_to_mmsc(c, pos);
                    hvfs_info(mmcc, "Create new connection for Server ID=%ld.\n",
                              c->sid);
                    break;
                }
            }
        } else
            break;
    } while (1);

    return pos;
}

void __free_mmsc(struct MMSConnection *c)
{
    xfree(c);
}

int recv_bytes(int sockfd, void *buf, int n)
{
	int i = 0;

    do {
		int a = recv(sockfd, buf + i, n - i, 0);

		if (a == -1) {
			return EMMCONNERR;
		}
		i += a;
		
		if (a == 0 && i < n) {
            hvfs_debug(mmcc, "recv EOF\n");
            return EMMCONNERR;
		}
    } while (i < n);

    return 0;
}

int send_bytes(int sockfd, void *buf, int n)
{
	int i = 0;
    
    do {
		int a = send(sockfd, buf + i, n - i, MSG_NOSIGNAL);
		if (a == -1) {
            return EMMCONNERR;
		}
		i += a;
    } while (i < n);

    return 0;
}

int recv_int(int sockfd) 
{
	int bytes, err = 0;

    err = recv_bytes(sockfd, &bytes, 4);
    if (err) {
        hvfs_err(mmcc, "recv_bytes() for length failed w/ %d.\n", err);
        goto out;
    }

    err = ntohl(bytes);
out:
    return err;
}

int send_int(int sockfd, int num)
{
    int bytes;

    bytes = htonl(num);
    
	return send_bytes(sockfd, &bytes, 4);
}

/* 
 * type@set@serverid@block@offset@length@disk
 */
int search_by_info(char *info, void **buf, size_t *length)
{
    /* split the info by @ */
    char *p = strdup(info), *n = NULL;
    char *x = p;
    long sid = -1;
    struct MMSConnection *c;
    struct MMSCSock *s;
    int err = 0, i;

    for (i = 0; i < 3; i++, p = NULL) {
        p = strtok_r(p, "@", &n);
        if (p != NULL) {
            switch (i) {
            case 2:
                /* serverId */
                sid = atol(p);
                break;
            default:
                break;
            }
        } else {
            err = EMMINVAL;
            goto out;
        }
    }
    xfree(x);

    c = __mmsc_lookup(sid);
    if (!c) {
        hvfs_err(mmcc, "lookup Server ID=%ld failed.\n", sid);
        err = EMMINVAL;
        goto out;
    }
    if (c->ttl <= 0) {
        err = EMMCONNERR;
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed.\n");
        err = EMMCONNERR;
        goto out;
    }

    {
        char header[4] = {
            (char)SEARCH,
            (char)strlen(info),
        };
        int count;

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed.\n");
            goto out_disconn;
        }
        
        err = send_bytes(s->sock, info, strlen(info));
        if (err) {
            hvfs_err(mmcc, "send info failed.\n");
            goto out_disconn;
        }
	
        count = recv_int(s->sock);
        if (count == -1) {
            err = EMMNOTFOUND;
            goto out_put;
        } else if (count < 0) {
            if (count == EMMCONNERR) {
                /* connection broken, do reconnect */
                goto out_disconn;
            }
        }
        
        *buf = xmalloc(count);
        if (!*buf) {
            hvfs_err(mmcc, "xmalloc() buffer %dB failed.\n", count);
            err = EMMNOMEM;
            goto out_disconn;
        }
        *length = (size_t)count;

        err = recv_bytes(s->sock, *buf, count);
        if (err) {
            hvfs_err(mmcc, "recv_bytes data content %dB failed.\n", count);
            xfree(*buf);
            goto out_disconn;
        }
    }

out_put:    
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

int search_mm_object(char *infos, void **buf, size_t *length)
{
    char *p = infos, *n = NULL;
    int err = 0, found = 0;
#ifdef DEBUG_LATENCY
    struct timeval tv;
    double begin, end;

    gettimeofday(&tv, NULL);
    begin = tv.tv_sec * 1000000.0 + tv.tv_usec;
#endif

    /* FIXME: we should random read from backend servers.
     */
    do {
        p = strtok_r(p, "#", &n);
        if (p != NULL) {
            /* handle this info */
            err = search_by_info(p, buf, length);
            if (err) {
                hvfs_err(mmcc, "search_by_info(%s) failed %d\n",
                         p, err);
            } else {
                found = 1;
                break;
            }
        } else {
            break;
        }
    } while (p = NULL, 1);

#ifdef DEBUG_LATENCY
    gettimeofday(&tv, NULL);
    end = tv.tv_sec * 1000000.0 + tv.tv_usec;

    {
        static int xx = 0;
        static double acc = 0.0;
        
        xx++;
        acc += (end - begin);
        if (xx % 100 == 0)
            hvfs_info(mmcc, "search mm obj AVG latency %lf us\n", acc / xx);
    }
#endif

    if (!found)
        return EMMNOTFOUND;
    else
        return 0;
}

int get_mm_object(char *set, char *md5, void **buf, size_t *length)
{
	redisReply* reply = NULL;
    int err = 0;
    struct redisConnection *rc = NULL;

    if (!set || !md5 || !buf || !length)
        return EMMINVAL;
    
    rc = getRC();
    if (!rc) {
        hvfs_err(mmcc, "getRC() failed\n");
        return EMMMETAERR;
    }
    
    reply = redisCMD(rc->rc, "hget %s %s", set, md5);

	if (reply == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
	}

	if (reply->type == REDIS_REPLY_NIL) {
        hvfs_err(mmcc, "%s@%s does not exist on MM Meta server.\n", set, md5);
        err = EMMNOTFOUND;
        goto out_free;
	}
    
    if (reply->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmcc, "hget %s %s failed w/ %s\n", set, md5, reply->str);
        err = EMMMETAERR;
        goto out_free;
    }

    err = search_mm_object(reply->str, buf, length);
    if (err) {
        hvfs_err(mmcc, "search_mm_object failed w/ %d\n", err);
        goto out_free;
    }

out_free:        
    freeReplyObject(reply);
out:
    __put_rc(rc);
    return err;
}

struct key2info *get_k2i(char *set, int *nr)
{
    redisReply *rpy = NULL;
    struct key2info *ki = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(mmcc, "getRC() failed\n");
        return NULL;
    }
    
    rpy = redisCMD(rc->rc, "hgetall %s", set);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }

    if (rpy->type == REDIS_REPLY_NIL) {
        hvfs_err(mmcc, "%s does not exist on MM Meta server.\n", set);
        err = EMMNOTFOUND;
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_ARRAY) {
        int i, j;

        *nr = rpy->elements / 2;
        ki = xzalloc(*nr * sizeof(*ki));
        if (!ki) {
            hvfs_err(mmcc, "alloc k2i failed, no memory.\n");
            err = EMMNOMEM;
            goto out_free;
        }
        
        for (i = 0, j = 0; i < rpy->elements; i+=2, j++) {
            ki[j].key = strdup(rpy->element[i]->str);
            ki[j].info = strdup(rpy->element[i + 1]->str);
        }
    }

out_free:
    freeReplyObject(rpy);
out:
    __put_rc(rc);

    if (err)
        ki = NULL;
    
    return ki;
}

static int do_put(long sid, char *set, char *name, void *buffer, 
                  size_t len, char **out)
{
    struct MMSConnection *c;
    struct MMSCSock *s;
    int err = 0;

    c = __mmsc_lookup(sid);
    if (!c) {
        hvfs_err(mmcc, "lookup Server ID=%ld failed.\n", sid);
        err = -EINVAL;
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed for Server ID=%ld.\n", sid);
        err = EMMCONNERR;
        goto out;
    }

    {
        int count = len, slen = strlen(set), nlen = strlen(name);
        char header[4] = {
            (char)SYNCSTORE,
            (char)slen,
            (char)nlen,
        };

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed.\n");
            goto out_disconn;
        }

        err = send_int(s->sock, count);
        if (err) {
            hvfs_err(mmcc, "send length failed.\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, set, slen);
        if (err) {
            hvfs_err(mmcc, "send set failed.\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, name, nlen);
        if (err) {
            hvfs_err(mmcc, "send name (or md5) failed.\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, buffer, count);
        if (err) {
            hvfs_err(mmcc, "send data content failed.\n");
            goto out_disconn;
        }

        /* ok, wait for reply now */
        count = recv_int(s->sock);
        if (count == -1) {
            err = EMMINVAL;
            goto out_put;
        } else if (count < 0) {
            if (count == EMMCONNERR) {
                /* connection broken, do reconnect */
                goto out_disconn;
            }
        }

        if (!*out) {
            *out = xzalloc(count + 1);
            if (!*out) {
                hvfs_err(mmcc, "xmalloc() buffer %dB failed.", count + 1);
                err = EMMNOMEM;
                goto out_disconn;
            }
        }

        err = recv_bytes(s->sock, *out, count);
        if (err) {
            hvfs_err(mmcc, "recv_bytes data content %dB failed.\n",
                     count);
            xfree(*out);
            goto out_disconn;
        }

        if (strncmp(*out, "#FAIL:", 6) == 0) {
            err = EMMMETAERR;
            goto out_put;
        }
    }

out_put:
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

static int do_put_iov(long sid, char *set, char *name, struct iovec *iov,
                      int iovlen, char **out)
{
    struct MMSConnection *c;
    struct MMSCSock *s;
    int err = 0, tlen = 0, i;

    for (i = 0; i < iovlen; i++) {
        tlen += iov[i].iov_len;
    }

    c = __mmsc_lookup(sid);
    if (!c) {
        hvfs_err(mmcc, "lookup Server ID=%ld failed.\n", sid);
        err = EMMINVAL;
        goto out;
 }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed.\n");
        err = EMMCONNERR;
        goto out;
    }

    {
        int count = tlen, slen = strlen(set), nlen = strlen(name);
        char header[4] = {
            (char)SYNCSTORE,
            (char)slen,
            (char)nlen,
        };

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed.\n");
            goto out_disconn;
        }

        err = send_int(s->sock, count);
        if (err) {
            hvfs_err(mmcc, "send length failed.\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, set, slen);
        if (err) {
            hvfs_err(mmcc, "send set failed.\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, name, nlen);
        if (err) {
            hvfs_err(mmcc, "send name (or md5) failed.\n");
            goto out_disconn;
        }

        for (i = 0; i < iovlen; i++) {
            err = send_bytes(s->sock, iov[i].iov_base, iov[i].iov_len);
            if (err) {
                hvfs_err(mmcc, "send content iov[%d] len=%ld failed.\n",
                         i, (u64)iov[i].iov_len);
                goto out_disconn;
            }
        }

        /* ok, wait for reply now */
        count = recv_int(s->sock);
        if (count == -1) {
            err = EMMINVAL;
            goto out_put;
        } else if (count < 0) {
            if (count == EMMCONNERR) {
                /* connection broken, do reconnect */
                goto out_disconn;
            }
        }

        if (!*out) {
            *out = xzalloc(count + 1);
            if (!*out) {
                hvfs_err(mmcc, "xmalloc() buffer %dB failed.", count + 1);
                err = EMMNOMEM;
                goto out_disconn;
            }
        }

        err = recv_bytes(s->sock, *out, count);
        if (err) {
            hvfs_err(mmcc, "recv_bytes data content %dB failed.\n",
                     count);
            xfree(*out);
            goto out_disconn;
        }

        if (strncmp(*out, "#FAIL:", 6) == 0) {
            err = EMMMETAERR;
            goto out_put;
        }
    }

out_put:
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

int __mmcc_put(char *set, char *name, void *buffer, size_t len,
               int dupnum, char **info)
{
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *n;
    time_t cur = time(NULL);
    int i, j, k, c = 0, total = 0, *ids = NULL, err = 0;

    srandom(cur);
    
    for (i = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(n, pos, &rh->h, hlist) {
            if (cur - n->ttl < 300)
                total++;
        }
        xlock_unlock(&rh->lock);
    }
    if (total == 0) {
        hvfs_warning(mmcc, "No valid MMServer to write?\n");
        return EMMNOMMS;
    }

    if (dupnum > total)
        dupnum = total;

    k = random() % total;

    ids = alloca(sizeof(int) * dupnum);

    for (i = 0, j = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(n, pos, &rh->h, hlist) {
            if (cur - n->ttl < 120 && j >= k && j < k + dupnum) {
                /* save it  */
                ids[c] = n->sid;
                c++;
            }
        }
        xlock_unlock(&rh->lock);
        if (c >= dupnum) break;
    }

    if (c < dupnum) {
        for (i = 0; i < g_rh_size; i++) {
            rh = g_rh + i;
            xlock_lock(&rh->lock);
            hlist_for_each_entry(n, pos, &rh->h, hlist) {
                if (cur - n->ttl < 120 && c < dupnum) {
                    /* save it */
                    ids[c] = n->sid;
                    c++;
                }
            }
            xlock_unlock(&rh->lock);
            if (c >= dupnum) break;
        }
    }

    hvfs_debug(mmcc, "Got active servers = %d to put for %s@%s\n", 
               c, set, name);

    /* ok, finally call do_put() */
    for (i = 0; i < c; i++) {
        char *out = NULL;
        int lerr = 0;
        
        lerr = do_put(ids[i], set, name, buffer, len, &out);
        if (lerr) {
            hvfs_err(mmcc, "do_put() on Server %d failed w/ %d, ignore\n",
                     ids[i], lerr);
            if (!err)
                err = lerr;
        } else {
            err = -1;
        }
        if (*info == NULL)
            *info = out;
        else
            xfree(out);
    }
    if (err == -1)
        err = 0;

    /* get the info now */
    {
        redisReply *rpy = NULL;
        struct redisConnection *rc = getRC();

        if (!rc) {
            hvfs_err(mmcc, "getRC() failed\n");
            goto out;
        }
        rpy = redisCMD(rc->rc, "hget %s %s", set, name);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            goto local_out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "%s/%s does not exist or internal error.\n",
                     set, name);
            goto local_out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            char *_t = NULL;
            int ilen = strlen(rpy->str) + 1;
            
            _t = xzalloc(ilen);
            if (!_t) {
                hvfs_err(mmcc, "alloc %dB failed for info, no memory.\n",
                         ilen);
            } else {
                xfree(*info);
                *info = _t;
                memcpy(_t, rpy->str, ilen - 1);
            }
            
        }
    local_out_free:
        freeReplyObject(rpy);
    local_out:
        putRC(rc);
    }

out:
    return err;
}

int __mmcc_put_iov(char *set, char *name, struct iovec *iov, int iovlen,
                   int dupnum, char **info)
{
    struct hlist_node *pos;
    struct regular_hash *rh;
    struct MMSConnection *n;
    time_t cur = time(NULL);
    int i, j, k, c = 0, total = 0, *ids = NULL, err = 0;

    srandom(cur);

    for (i = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(n, pos, &rh->h, hlist) {
            if (cur - n->ttl < 300)
                total++;
        }
        xlock_unlock(&rh->lock);
    }
    if (total == 0) {
        hvfs_warning(mmcc, "No valid MMServer to write?\n");
        return EMMNOMMS;
    }

    if (dupnum > total)
        dupnum = total;

    k = random() % total;

    ids = alloca(sizeof(int) * dupnum);

    for (i = 0, j = 0; i < g_rh_size; i++) {
        rh = g_rh + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(n, pos, &rh->h, hlist) {
            if (cur - n->ttl < 120 && j >= k && j < k + dupnum) {
                /* save it  */
                ids[c] = n->sid;
                c++;
            }
        }
        xlock_unlock(&rh->lock);
    }

    if (c < dupnum) {
        for (i = 0; i < g_rh_size; i++) {
            rh = g_rh + i;
            xlock_lock(&rh->lock);
            hlist_for_each_entry(n, pos, &rh->h, hlist) {
                if (cur - n->ttl < 120 && c < dupnum) {
                    /* save it */
                    ids[c] = n->sid;
                    c++;
                }
            }
            xlock_unlock(&rh->lock);
        }
    }

    hvfs_debug(mmcc, "Got active servers = %d to put for %s@%s\n", 
               c, set, name);

    /* ok, finally call do_put_iov() */
    for (i = 0; i < c; i++) {
        char *out = NULL;
        int lerr = 0;
        
        lerr = do_put_iov(ids[i], set, name, iov, iovlen, &out);
        if (lerr) {
            hvfs_err(mmcc, "do_put() on Server %d failed w/ %d, ignore\n",
                     ids[i], lerr);
            if (!err)
                err = lerr;
        } else {
            err = -1;
        }
        if (*info == NULL)
            *info = out;
        else
            xfree(out);
    }
    if (err == -1)
        err = 0;

    /* get the info now */
    {
        redisReply *rpy = NULL;
        struct redisConnection *rc = getRC();

        if (!rc) {
            hvfs_err(mmcc, "getRC() failed\n");
            goto out;
        }
        rpy = redisCMD(rc->rc, "hget %s %s", set, name);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            goto local_out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "%s/%s does not exist or internal error.\n",
                     set, name);
            goto local_out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            char *_t = NULL;
            int ilen = strlen(rpy->str) + 1;
            
            _t = xzalloc(ilen);
            if (!_t) {
                hvfs_err(mmcc, "alloc %dB failed for info, no memory.\n",
                         ilen);
            } else {
                xfree(*info);
                *info = _t;
                memcpy(_t, rpy->str, ilen - 1);
            }
            
        }
    local_out_free:
        freeReplyObject(rpy);
    local_out:
        putRC(rc);
    }

out:
    return err;
}

/* Return value: 
 * 0   -> not duplicated; 
 * > 0 -> duplicated N times;
 * < 0 -> error
 */
static int __dup_detect(char *set, char *name, char **info)
{
    redisReply *rpy = NULL;
    struct redisConnection *rc = NULL;
    int err = 0;

    if (g_conf.mode & MMSCONF_NODUP) {
        return 0;
    }
    rc = getRC();
    if (!rc) {
        hvfs_err(mmcc, "getRC() failed\n");
        err = EMMMETAERR;
        goto out;
    }
    if ((g_conf.mode & MMSCONF_DUPSET) &&
        (g_conf.mode & MMSCONF_DEDUP))
        rpy = redisCMD(rc->rc, "evalsha %s 1 %s %s",
                       g_ops[__CLIENT_R_OP_DUP_DETECT].sha,
                       set, 
                       name);
    else
        rpy = redisCMD(rc->rc, "hget %s %s", set, name);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        err = EMMINVAL;
        freeRC(rc);
        goto out_put;
    }
    if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
        hvfs_warning(mmcc, "%s@%s does not exist or internal error.\n",
                     set, name);
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        char *p = rpy->str;
        int dupnum = 0;

        /* count # now */
        while (*p != '\0') {
            if (*p == '#')
                dupnum++;
            p++;
        }
        err = dupnum + 1;
        *info = strdup(rpy->str);
    }
    
out_free:
    freeReplyObject(rpy);
out_put:
    putRC(rc);
out:
    return err;
}

static int __log_dupinfo(char *set, char *name)
{
    redisReply *rpy = NULL;
    struct redisConnection *rc = getRC();
    int err = 0;

    if (!rc) {
        hvfs_err(mmcc, "getRC() failed\n");
        err = EMMMETAERR;
        goto out;
    }
    rpy = redisCMD(rc->rc, "hincrby mm.dedup.info %s@%s 1",
                   set, name);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        err = EMMINVAL;
        freeRC(rc);
        goto out_put;
    }
    if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(mmcc, "%s@%s does not exist or internal error.\n",
                 set, name);
        err = EMMINVAL;
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        /* it is ok, ignore result */
        ;
    }

out_free:
    freeReplyObject(rpy);
out_put:
    putRC(rc);
out:
    return err;
}

void mmcc_put_R(char *key, void *content, size_t len, struct mres *mr)
{
    char *info = NULL, *set = NULL, *name = NULL;
    int err = 0;

    memset(mr, 0, sizeof(*mr));
    if (!key || !content || len <= 0)
        return;

    /* split by @ */
    {
        char *p = strdup(key), *n = NULL;
        char *q = p;
        int i = 0;

        for (i = 0; i < 2; i++, p = NULL) {
            p = strtok_r(p, "@", &n);
            if (p != NULL) {
                switch (i) {
                case 0:
                    set = strdup(p);
                    break;
                case 1:
                    name = strdup(p);
                    break;
                }
            } else {
                goto out_free;
            }
        }
    out_free:
        xfree(q);
    }

    /* dup detection */
    err = __dup_detect(set, name, &info);
    if (err > 0) {
        mr->flag |= MR_FLAG_DUPED;
        /* do logging */
        if (g_conf.logdupinfo) {
            __log_dupinfo(set, name);
        }
        /* if larger than dupnum, then do NOT put */
        if (err >= g_conf.dupnum) {
            hvfs_debug(mmcc, "DETECT dupnum=%d >= %d, do not put actually.\n",
                       err, g_conf.dupnum);
            goto out_free2;
        }
    }

    err = __mmcc_put(set, name, content, len, g_conf.dupnum, &info);
    if (err) {
        hvfs_err(mmcc, "__mmcc_put() %s %s failed w/ %d\n",
                 set, name, err);
        goto out_free2;
    }
    hvfs_debug(mmcc, "mmcc_put() key=%s info=%s\n",
               key, info);

out_free2:
    xfree(set);
    xfree(name);

    mr->info = info;
}

char *mmcc_put(char *key, void *content, size_t len)
{
    struct mres mr;

    mmcc_put_R(key, content, len, &mr);

    return mr.info;
}

void mmcc_put_iov_R(char *key, struct iovec *iov, int iovlen, struct mres *mr)
{
    char *info = NULL, *set = NULL, *name = NULL;
    int err = 0;

    memset(mr, 0, sizeof(*mr));
    if (!key || !iov || iovlen <= 0)
        return;

    /* split by @ */
    {
        char *p = strdup(key), *n = NULL;
        char *q = p;
        int i = 0;

        for (i = 0; i < 2; i++, p = NULL) {
            p = strtok_r(p, "@", &n);
            if (p != NULL) {
                switch (i) {
                case 0:
                    set = strdup(p);
                    break;
                case 1:
                    name = strdup(p);
                    break;
                }
            } else {
                goto out_free;
            }
        }
    out_free:
        xfree(q);
    }

    /* dup detection */
    err = __dup_detect(set, name, &info);
    if (err > 0) {
        mr->flag |= MR_FLAG_DUPED;
        /* do logging */
        if (g_conf.logdupinfo) {
            __log_dupinfo(set, name);
        }
        /* if larger than dupnum, then do NOT put */
        if (err >= g_conf.dupnum) {
            hvfs_debug(mmcc, "DETECT dupnum=%d >= %d, do not put actually.\n",
                       err, g_conf.dupnum);
            goto out_free2;
        }
    }

    err = __mmcc_put_iov(set, name, iov, iovlen, g_conf.dupnum, &info);
    if (err) {
        hvfs_err(mmcc, "__mmcc_put() %s %s failed w/ %d\n",
                 set, name, err);
        goto out_free2;
    }
    hvfs_debug(mmcc, "mmcc_put() key=%s info=%s\n",
               key, info);

out_free2:
    xfree(set);
    xfree(name);

    mr->info = info;
}

char *mmcc_put_iov(char *key, struct iovec *iov, int iovlen)
{
    struct mres mr;

    mmcc_put_iov_R(key, iov, iovlen, &mr);

    return mr.info;
}

int __del_mm_set(char *host, int port, char *set)
{
    struct MMSConnection *c;
    struct MMSCSock *s;
    int err = 0;

    c = __mmsc_lookup_byname(host, port);
    if (!c) {
        hvfs_err(mmcc, "lookup Server %s:%d failed.\n", host, port);
        err = -EINVAL;
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed for Server %s:%d.\n",
                 host, port);
        err = EMMCONNERR;
        goto out;
    }

    {
        int slen = strlen(set);
        char header[4] = {
            (char)DELSET,
            (char)slen,
        };
        char r = 0;

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed.\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, set, slen);
        if (err) {
            hvfs_err(mmcc, "send set failed.\n");
            goto out_disconn;
        }

        /* ok, wait for reply now */
        err = recv_bytes(s->sock, &r, 1);
        if (err) {
            hvfs_err(mmcc, "recv_bytes result byte failed.\n");
            goto out_disconn;
        }

        if (r != 1) {
            hvfs_err(mmcc, "delete set %s failed on server %s:%d\n",
                     set, host, port);
            err = -EFAULT;
            goto out_put;
        }
    }
out_put:
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

int del_mm_set_on_redis(char *set)
{
    redisReply *rpy = NULL;
    char **keys = NULL;
    int err = 0, knr = 0, i;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(mmcc, "getRC() failed\n");
        return EMMMETAERR;
    }

    /* Step 1: get keys of set.* */
    {
        rpy = redisCMD(rc->rc, "keys %s*", set);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out_put;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "MM Meta server error %s\n", rpy->str);
            err = -EINVAL;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_ARRAY) {
            knr = rpy->elements;
            keys = alloca(sizeof(char *) * rpy->elements);

            for (i = 0; i < rpy->elements; i++) {
                keys[i] = strdup(rpy->element[i]->str);
            }
        }
    out_free:
        freeReplyObject(rpy);
    }

    /* Step 2: del all keys related this set */
    for (i = 0; i < knr; i++) {
        rpy = redisCMD(rc->rc, "del %s", keys[i]);
        if (rpy == NULL) {
            hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
            freeRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(mmcc, "MM Meta server error %s\n", rpy->str);
            err = -EINVAL;
            goto out_free2;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 1) {
                hvfs_debug(mmcc, "delete set %s ok.\n", keys[i]);
            } else {
                hvfs_debug(mmcc, "delete set %s failed, not exist.\n", keys[i]);
            }
        }
    out_free2:
        freeReplyObject(rpy);
    }

out:
    for (i = 0; i < knr; i++) {
        xfree(keys[i]);
    }
out_put:
    __put_rc(rc);

    return err;
}

int del_mm_set_on_mms(char *set)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(mmcc, "getRC() failed\n");
        return EMMMETAERR;
    }

    rpy = redisCMD(rc->rc, "smembers %s.srvs", set);
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        freeRC(rc);
        err = EMMMETAERR;
        goto out;
    }

    if (rpy->type == REDIS_REPLY_NIL) {
        hvfs_err(mmcc, "%s does not exist on MM Meta server.\n", set);
        err = EMMNOTFOUND;
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_ARRAY) {
        int i;

        for (i = 0; i < rpy->elements; i++) {
            char *p = strdup(rpy->element[i]->str), *n = NULL;
            char *q = p;
            char *host = NULL;
            int j = 0, port = -1;

            for (j = 0; j < 2; j++, p = NULL) {
                p = strtok_r(p, ":", &n);
                if (p != NULL) {
                    switch (j) {
                    case 0:
                        host = strdup(p);
                        break;
                    case 1:
                        port = atoi(p);
                        break;
                    }
                } else {
                    goto out_freep;
                }
            }
            /* send DEL to MMServer */
            __del_mm_set(host, port, set);

            xfree(host);
        out_freep:
            xfree(q);
        }
    }
    
out_free:
    freeReplyObject(rpy);
out:
    __put_rc(rc);
    
    return err;
}

int del_mm_set(char *set)
{
    int err = 0;

    err = del_mm_set_on_mms(set);
    if (err) {
        hvfs_err(mmcc, "delete set %s on MMServer failed w/ %d\n",
                 set, err);
        goto out;
    }
    err = del_mm_set_on_redis(set);
    if (err) {
        hvfs_err(mmcc, "delete set %s on MM Meta failed w/ %d\n",
                 set, err);
        goto out;
    }
    
out:
    return err;
}

/*
 * Return value: 0 -> not in; >0 -> is in; <0 -> error
 */
int __is_in_redis(char *set)
{
    long this_ts = LONG_MAX;
    char *t = set;

    if (!isdigit(set[0]))
        t = set + 1;
    this_ts = atol(t);

    if (this_ts > g_ckpt_ts)
        return 1;
    else
        return 0;
}

int get_ss_object(char *set, char *md5, void **buf, size_t *length)
{
    struct MMSConnection *c ;
    struct MMSCSock *s;
    int err = 0;

    if (!set || !md5 || !buf || !length)
        return EMMINVAL;

    if (g_ssid < 0)
        return EMMINVAL;

    c = __mmsc_lookup(g_ssid);
    if (!c) {
        hvfs_err(mmcc, "lookup SS Server ID=%ld failed.\n", g_ssid);
        err = EMMINVAL;
        goto out;
    }
    if (c->ttl <= 0) {
        err = EMMCONNERR;
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed.\n");
        err = EMMCONNERR;
        goto out;
    }

    {
        char setlen = (char)strlen(set);
        char md5len = (char)strlen(md5);
        char header[4] = {
            (char)XSEARCH,
            (char)setlen,
            (char)md5len,
        };
        int count, xerr = 0;

        err = send_bytes(s->sock, header, 4);
        if (err) {
            hvfs_err(mmcc, "send header failed.\n");
            goto out_disconn;
        }

        err = send_bytes(s->sock, set, setlen);
        if (err) {
            hvfs_err(mmcc, "send set failed.\n");
            goto out_disconn;
        }
        err = send_bytes(s->sock, md5, md5len);
        if (err) {
            hvfs_err(mmcc, "send md5 failed.\n");
            goto out_disconn;
        }

        count = recv_int(s->sock);
        if (count == -1) {
            err = EMMNOTFOUND;
            goto out_put;
        } else if (count < 0) {
            /* oo, we got redirect_info */
            count = -count;
            xerr = EREDIRECT;
        }

        *buf = xmalloc(count);
        if (!*buf) {
            hvfs_err(mmcc, "xmalloc() buffer %dB failed.\n", count);
            err = EMMNOMEM;
            goto out_disconn;
        }
        *length = (size_t)count;

        err = recv_bytes(s->sock, *buf, count);
        if (err) {
            hvfs_err(mmcc, "recv_bytes data content %dB failed.\n", count);
            xfree(*buf);
            goto out_disconn;
        }
        if (xerr) err = xerr;
    }

out_put:
    __put_inuse_sock(c, s);

out:
    return err;
out_disconn:
    __del_from_mmsc(c, s);
    __free_mmscsock(s);
    err = EMMCONNERR;
    goto out;
}

