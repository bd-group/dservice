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
#include "hvfs_u.h"
#include "xlist.h"
#include "xlock.h"
#include "memory.h"
#include "xhash.h"

#define HVFS_TRACING
#include "tracing.h"

HVFS_TRACING_INIT();

unsigned int hvfs_mmcc_tracing_flags = 0xffffffff;

#define SYNCSTORE 1
#define SEARCH 2
#define ASYNCSTORE 4
#define SERVERINFO 5

char *redisHost = NULL;
int redisPort = -1;

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
#define MMSCSOCK_MAX            5
    int sock_nr;
    int port;
};

struct redisConnection
{
    time_t ttl;
    pid_t tid;
    struct hlist_node hlist;
    atomic_t ref;

    redisContext *rc;
    xlock_t lock;
};

/* quick lookup from THREADID to redisContext */
static struct regular_hash *g_rcrh = NULL;
static int g_rcrh_size = 1024;

/* quick lookup from SID to MMSConnection */
static struct regular_hash *g_rh = NULL;
static int g_rh_size = 32;

static sem_t g_timer_sem;
static pthread_t g_timer_thread = 0;
static int g_timer_thread_stop = 0;

void __clean_rcs(time_t cur);

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
    }

    hvfs_debug(mmcc, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

static void __itimer_default(int signo, siginfo_t *info, void *arg)
{
    sem_post(&g_timer_sem);
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

    err = setup_timers(30);
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
        rc->ttl = time(NULL);
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
                       (tpos->ttl + 3600), cur, atomic_read(&tpos->ref));
            if ((tpos->ttl + 3600 < cur) && atomic_read(&tpos->ref) == 0) {
                hlist_del_init(&tpos->hlist);
                hvfs_debug(mmcc, "Clean long living RC for TID %ld\n", 
                           (long)tpos->tid);
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
    redisReply *rpy = NULL;
    int err = 0;

    rpy = redisCMD(rc->rc, "zrange mm.active 0 -1 withscores");
    if (rpy == NULL) {
        hvfs_err(mmcc, "read from MM Meta failed: %s\n", rc->rc->errstr);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ARRAY) {
        int i = 0, j;

        /* alloc server list */
        struct MMSConnection *c;

        for (j = 0; j < rpy->elements; j += 2) {
            char *p = rpy->element[j]->str, *n, *hostname;
            int port;
            long sid;

            p = strtok_r(p, ":", &n);
            if (p) {
                hostname = strdup(p);
            } else {
                hvfs_err(mmcc, "strtok_r for MMServer failed, ignore.\n");
                continue;
            }
            p = strtok_r(NULL, ":", &n);
            if (p) {
                port = atoi(p);
            } else {
                hvfs_err(mmcc, "strtok_r for MMServer port failed, ignore.\n");
                xfree(hostname);
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
                    continue;
                }
            }

            xlock_lock(&c->lock);
            xfree(c->hostname);
            c->hostname = hostname;
            c->port = port;
            c->sid = sid;
            xlock_unlock(&c->lock);
            
            hvfs_info(mmcc, "Update MMServer ID=%ld %s:%d\n", c->sid,
                      hostname, port);
            __mmsc_insert(c);
            i++;
        }
        hvfs_info(mmcc, "Got %d MMServer from Meta Server.\n", i);
    }

    freeReplyObject(rpy);
out:
    return err;
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
            hvfs_err(mmcc, "can't connect to redis at %s:%d %s\n", 
                     redisHost, redisPort, rc->rc->errstr);
            redisFree(rc->rc);
            err = EMMMETAERR;
            __put_rc(rc);
            goto out;
        }
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
            rc->rc = redisConnect(redisHost, redisPort);
            if (rc->rc->err) {
                int n = 2;

                hvfs_err(mmcc, "can't connect to redis at %s:%d %s\n",
                         redisHost, redisPort, rc->rc->errstr);
                redisFree(rc->rc);
                rc->rc = NULL;
                __put_rc(rc);
                err = EMMMETAERR;
                do {
                    n = sleep(n);
                } while (n);
                goto out;
            }
            /* update server array */
            err = update_mmserver(rc);
            if (err) {
                __put_rc(rc);
                hvfs_err(mmcc, "update_mmserver() failed w/ %d\n", err);
                goto out;
            }
        }
    }

out:
    if (err)
        rc = NULL;
    
    return rc;
}

void putRC(struct redisConnection *rc)
{
    __put_rc(rc);
}

static inline void freeRC(struct redisConnection *rc)
{
    redisFree(rc->rc);
    rc->rc = NULL;
}

struct MMSCSock *__get_free_sock(struct MMSConnection *c)
{
    struct MMSCSock *pos;
    int found = 0;

    do {
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
            if (c->sock_nr < MMSCSOCK_MAX) {
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

                        hvfs_err(mmcc, "create socket failed w/ %s(%d)\n",
                                 strerror(errno), errno);
                        __free_mmscsock(pos);
                        do {
                            n = sleep(n);
                        } while (n);
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
                        hvfs_err(mmcc, "resolve nodename:%s failed!\n", hostname);
                        __free_mmscsock(pos);
                        sleep(1);
                        continue;
                    }

                    dest_addr.sin_family = AF_INET;
                    dest_addr.sin_port = htons(port);
                    dest_addr.sin_addr.s_addr = ((struct in_addr *)
                                                 (hptr->h_addr))->s_addr;
                    bzero(&(dest_addr.sin_zero), 8);

                    err = connect(sock, (struct sockaddr *)&dest_addr, 
                                  sizeof(struct sockaddr));
                    if (err) {
                        hvfs_err(mmcc, "Connection to server failed w/ %s(%d).\n",
                                 strerror(errno), errno);
                        __free_mmscsock(pos);
                        sleep(1);
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
    char *p = strdup(info), *n;
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
        goto out;
    }
    s = __get_free_sock(c);
    if (!s) {
        hvfs_err(mmcc, "get free socket failed.\n");
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
            goto out_put;
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
    char *p = infos, *n;
    int err = 0, found = 0;
#ifdef DEBUG_LATENCY
    struct timeval tv;
    double begin, end;

    gettimeofday(&tv, NULL);
    begin = tv.tv_sec * 1000000.0 + tv.tv_usec;
#endif

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

int get_mm_object(char* set, char* md5, void **buf, size_t* length)
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
        hvfs_err(mmcc, "%s.%s does not exist on MM Meta server.\n", set, md5);
        err = EMMNOTFOUND;
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
