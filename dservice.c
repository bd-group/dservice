/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-05-13 16:56:58 macan>
 *
 */

#include "common.h"
#include "jsmn.h"
#include "lib/memory.h"

#define MAX_FILTERS     10
struct dservice_conf
{
    char *sdfilter[MAX_FILTERS];
    char *mpfilter[MAX_FILTERS];
    int sdfilter_len;
    int mpfilter_len;
    int dscan_interval;         /* dev scan interval */
    int hb_interval;
    int mr_interval;
    int fl_interval; // fail interval
    int fl_max_retry;
    int max_keeping_days;
    int enable_dscan;
    int enable_audit;
    int drop_rep_threshold;
    int drop_rep_unit;
    char *addr_filter;
    char *data_path;
    long rep_max_ticks;
    long del_max_ticks;
};

#define DS_REP_MAX_TICKS     7200
#define DS_DEL_MAX_TICKS     600
static struct dservice_conf g_ds_conf = {
    .sdfilter = {
        "sda",
    },
    .sdfilter_len = 1,
    .mpfilter = {
        "/",
        "/boot",
    },
    .mpfilter_len = 2,
    .dscan_interval = 1800,
    .hb_interval = 5,
    .mr_interval = 10,
    .fl_interval = 3600,
    .fl_max_retry = 100,
    .max_keeping_days = 30,
    .enable_dscan = 0,
    .enable_audit = 0,
    .drop_rep_threshold = 3000,
    .drop_rep_unit = 2800,
    .addr_filter = NULL,
    /* FIXIME: change it to data */
    .data_path = "data",
    /* set max ticks to 7200, means at least 2 hours */
    .rep_max_ticks = DS_REP_MAX_TICKS,
    .del_max_ticks = DS_DEL_MAX_TICKS,
};

struct dev_scan_context
{
    char *devid;
    char *mp;
    int level;

    int prob;
    int nr;
};

struct dservice_info
{
    atomic64_t qrep;            /* queued reps */
    atomic64_t hrep;            /* handling reps */
    atomic64_t drep;            /* done reps (not reported) */

    atomic64_t qdel;            /* queued dels */
    atomic64_t hdel;            /* handling dels */
    atomic64_t ddel;            /* done dels (not reported) */

    atomic64_t tver;            /* total verify SFLs */
    atomic64_t tvyr;            /* total verify reply */

    long upts;                  /* startup timestamp */
    long uptime;                /* system uptime */
    long recvts;                /* last recv ok timestamp */
    double loadavg[3];          /* system loadavg */

    atomic_t updated;
};

struct thread_args
{
    int tid;
};

static struct dservice_info g_di;
static int g_rep_thread_nr = 1;
static int g_del_thread_nr = 1;

static sem_t g_timer_sem;
static sem_t g_main_sem;
static sem_t g_rep_sem;
static sem_t g_del_sem;
static sem_t g_async_recv_sem;
static sem_t g_dscan_sem;
static sem_t g_audit_sem;
static sem_t g_liveness_sem;

static pthread_t g_timer_thread = 0;
static pthread_t *g_rep_thread = NULL;
static pthread_t *g_del_thread = NULL;
static pthread_t g_async_recv_thread = 0;
static pthread_t g_dscan_thread = 0;
static pthread_t g_audit_thread = 0;
static pthread_t g_liveness_thread = 0;

static int g_timer_thread_stop = 0;
static int *g_rep_thread_stop = NULL;
static int *g_del_thread_stop = NULL;
static int g_main_thread_stop = 0;
static int g_async_recv_thread_stop = 0;
static int g_dscan_thread_stop = 0;
static int g_audit_thread_stop = 0;
static int g_liveness_thread_stop = 0;

static atomic64_t *g_rep_thread_ticks = 0;
static atomic64_t *g_del_thread_ticks = 0;

static char g_audit_header[128];

static char *g_devtype = NULL;

static int g_sockfd;
static struct sockaddr_in g_server;
static char *g_server_str = NULL;
static int g_port = 20202;

static struct sockaddr_in g_serverR;

static int g_specify_dev = 0;
static char *g_dev_str = NULL;
static char *g_hostname = NULL;

static xlock_t g_rep_lock;
static LIST_HEAD(g_rep);
static xlock_t g_del_lock;
static LIST_HEAD(g_del);
static xlock_t g_verify_lock;
static LIST_HEAD(g_verify);
static xlock_t g_dtrace_lock;
static LIST_HEAD(g_dtrace);

static int g_dtrace_update = 0;

#define NORMAL_HB_MODE          0
#define QUICK_HB_MODE           1
#define QUICK_HB_MODE2          10

static int g_rep_mkdir_on_nosuchfod = 0;
static int g_dev_scan_prob = 100;

/* rsync cmd: rsync -rp --partial (-z)
 * V1: if use timeout, then replace it as: timeout 1h rsync -rp --partial (-z)
 * timeout 1h scp -qpr
 * V2: if use timeout, use -O 1h
 */
static char *g_copy_cmd = "scp -qpr";
static int g_is_rsync = 0;

/*
 * timeout return 124 for truely timeout (network overload? or something else)
 */
static char *g_timeout_str = "timeout 1h";

/* dpi for heartbeat using
 */
static xlock_t g_dpi_lock;
static struct disk_part_info *g_dpi = NULL;
static int g_nr = 0;

static int __get_sysinfo()
{
    struct sysinfo s;
    static time_t last = 0;
    time_t cur = time(NULL);
    int err, i;

    if (last == 0) last = cur;
    if (cur - last < 0) {
        hvfs_warning(lib, "Detect timestamp adjust backward, "
                     "reset sysinfoTS.\n");
        last = cur;
    }
    
    memset(&s, 0, sizeof(s));
    err = sysinfo(&s);
    if (err) {
        hvfs_err(lib, "sysinfo() failed w/ %s(%d)\n",
                 strerror(errno), errno);
        return 0;
    }
    g_di.uptime = s.uptime;
    for (i = 0; i < 3; i++) {
        g_di.loadavg[i] = (double)s.loads[i] / (1 << SI_LOAD_SHIFT);
    }
    if ((atomic64_read(&g_di.qrep) +
         atomic64_read(&g_di.hrep) +
         atomic64_read(&g_di.drep) +
         atomic64_read(&g_di.qdel) +
         atomic64_read(&g_di.hdel) +
         atomic64_read(&g_di.ddel)) != 0 ||
        g_di.loadavg[0] > 10) {
        atomic_set(&g_di.updated, 1);
        return 1;
    } else if (atomic_read(&g_di.updated)) {
        atomic_set(&g_di.updated, 0);
        return 1;
    } else if (cur - last >= 60) {
        return 1;
    } else
        return 0;
}

static void __sigaction_default(int signo, siginfo_t *info, void *arg)
{
    if (signo == SIGSEGV) {
        hvfs_info(lib, "Recv %sSIGSEGV%s %s\n",
                  HVFS_COLOR_RED,
                  HVFS_COLOR_END,
                  SIGCODES(info->si_code));
        lib_segv(signo, info, arg);
    } else if (signo == SIGBUS) {
        hvfs_info(lib, "Recv %sSIGBUS%s %s\n",
                  HVFS_COLOR_RED,
                  HVFS_COLOR_END,
                  SIGCODES(info->si_code));
        lib_segv(signo, info, arg);
    } else if (signo == SIGHUP || signo == SIGINT || signo == SIGTERM) {
        if (!g_main_thread_stop) {
            hvfs_info(lib, "Exit DService, please wait ...\n");
            g_main_thread_stop = 1;
            sem_post(&g_main_sem);
        } else {
            hvfs_info(lib, "Please be patient, waiting for socket timeout.\n");
        }
    }
}

static int __init_signal(void)
{
    struct sigaction ac;
    int err;

    ac.sa_sigaction = __sigaction_default;
    err = sigemptyset(&ac.sa_mask);
    if (err) {
        err = errno;
        goto out;
    }
    ac.sa_flags = SA_SIGINFO;

#ifndef UNIT_TEST
    err = sigaction(SIGTERM, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGHUP, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGINT, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGSEGV, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGBUS, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGQUIT, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGUSR1, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
#endif

out:
    return err;
}

/* use global hint to filter a better IP address
 */
static void __convert_host_to_ip(char *host, char *ip)
{
    struct hostent *he;
    int copied = 0, i;

    if (!g_ds_conf.addr_filter) {
        strcpy(ip, host);
        return;
    }

    he = gethostbyname(host);
    if (!he) {
        hvfs_err(lib, "gethostbyname(%s) failed w/ %s(%d)\n",
                 host, strerror(errno), errno);
        goto out;
    }
    for (i = 0; i < he->h_length; i++) {
        struct sockaddr_in sa;

        sa.sin_addr.s_addr = *((unsigned long *)he->h_addr_list[i]);
        inet_ntop(AF_INET, &(sa.sin_addr), ip, NI_MAXHOST);
        if (strstr(ip, g_ds_conf.addr_filter) != NULL) {
            /* ok, use this IP */
            copied = 1;
            break;
        }
    }

out:    
    if (!copied) {
        strcpy(ip, host);
    }
}

int get_disks(struct disk_info **di, int *nr, char *label)
{
    char buf[1024];
    char *line = NULL;
    size_t len = 0;
    FILE *f;
    int err = 0, lnr = 0;

    sprintf(buf, GET_GL_DISK_SN, label, label);
    f = popen(buf, "r");
    if (f == NULL) {
        err = -errno;
        hvfs_err(lib, "popen(GET_GL_DISK_SN) failed w/ %s\n",
                 strerror(errno));
    } else {
        *di = NULL;
        *nr = 0;

        /* read from the result stream */
        while ((lnr = getline(&line, &len, f)) != -1) {
            /* ok, parse the results */
            if (lnr > 2) {
                char *p, *sp;
                int i;

                for (i = 0, p = line; ++i; p = NULL) {
                    p = strtok_r(p, " \n", &sp);
                    if (!p)
                        break;
                    hvfs_info(lib, "2 %d %s\n", i, p);
                    switch (i) {
                    case 1:
                        /* check whether it is OK */
                        if (strncmp(p, "OK", 2) != 0) {
                            /* bad */
                            hvfs_err(lib, "BAD line w/ '%s', ignore it\n", p);
                            goto next_line;
                        }
                        /* alloc a new disk_info entry */
                        {
                            struct disk_info *ndi;

                            ndi = xrealloc(*di, (*nr + 1) * sizeof(*ndi));
                            if (!ndi) {
                                hvfs_err(lib, "xrealloc disk_info failed.\n");
                                goto next_line;
                            }
                            *di = ndi;
                            *nr = *nr + 1;
                        }
                        break;
                    case 2:
                        /* it is dev_sn */
                        (*di)[*nr - 1].dev_sn = strdup(p);
                        break;
                    case 3:
                        /* it is dev id */
                        (*di)[*nr - 1].dev_id = strdup(p);
                        break;
                    }
                }
            }
        next_line:;
        }
        free(line);
    }
    pclose(f);
    
    return err;
}

void di_free(struct disk_info *di, int nr)
{
    int i;
    
    for (i = 0; i < nr; i++) {
        xfree(di[i].dev_sn);
        xfree(di[i].dev_id);
    }
    
    xfree(di);
}

int get_disk_stats(char *dev, long *rnr, long *wnr, long *enr)
{
    char buf[1024];
    char *line = NULL;
    size_t len = 0;
    FILE *f;
    int err = 0, lnr = 0, devlen = strlen(dev);

    if (devlen == 3) {
        /* this is the whole disk */
        sprintf(buf, GET_RW_SECTORS, dev);
    } else if (isdigit(dev[devlen - 1])) {
        /* this might be one partition */
        char wdev[32];

        strcpy(wdev, dev);
        wdev[3] = '\0';
        sprintf(buf, GET_RW_SECTORS_EXT, wdev, dev);
    } else {
        /* BAD device name, not ata,scsi? ignore them */
        return -EINVAL;
    }

    f = popen(buf, "r");

    if (f == NULL) {
        err = -errno;
        hvfs_err(lib, "popen(GET_RW_SECTORS) failed w/ %s\n",
                 strerror(errno));
        goto out;
    } else {
        if (rnr != NULL)
            *rnr = 0;
        if (wnr != NULL)
            *wnr = 0;
        if (enr != NULL)
            *enr = 0;
        /* read from the result stream */
        while ((lnr = getline(&line, &len, f)) != -1) {
            /* ok, parse the results */
            if (lnr > 2) {
                char *p, *sp;
                int i;

                for (i = 0, p = line; ++i; p = NULL) {
                    p = strtok_r(p, " \n", &sp);
                    if (!p)
                        break;
                    hvfs_debug(lib, "2 %d %s\n", i, p);
                    switch (i) {
                    case 1:
                        /* read sector nr */
                        if (rnr != NULL)
                            *rnr = atol(p);
                        break;
                    case 2:
                        /* write sector nr */
                        if (wnr != NULL)
                            *wnr = atol(p);
                        break;
                    case 3:
                        /* IO pending time */
                        if (enr != NULL)
                            *enr = atol(p);
                        break;
                    }
                    hvfs_debug(lib, "2 %d %s FFF\n", i, p);
                }
            }
        }
        free(line);
    }
    pclose(f);

out:
    return err;
}

int get_disk_parts_sn(struct disk_part_info **dpi, int *nr, char *label)
{
    char buf[1024];
    char *line = NULL;
    size_t len = 0;
    FILE *f;
    int err = 0, lnr = 0;

    sprintf(buf, GET_GL_DISKPART_SN, label, label);
    f = popen(buf, "r");

    if (f == NULL) {
        err = -errno;
        hvfs_err(lib, "popen(GET_GL_DISKPART_SN) failed w/ %s\n",
                 strerror(errno));
        goto out;
    } else {
        /* read from the result stream */
        while ((lnr = getline(&line, &len, f)) != -1) {
            /* ok, parse the results */
            if (lnr > 2) {
                char *p, *sp;
                int i, filtered = 0;

                for (i = 0, p = line; ++i; p = NULL) {
                    p = strtok_r(p, " \n", &sp);
                    if (!p)
                        break;
                    hvfs_debug(lib, "2 %d %s\n", i, p);
                    switch (i) {
                    case 1:
                        /* check whether it is OK */
                        if (strncmp(p, "OK", 2) != 0) {
                            /* bad */
                            hvfs_err(lib, "BAD line w/ '%s', ignore it\n", p);
                            goto next_line;
                        }
                        /* alloc a new disk_part_info entry */
                        {
                            struct disk_part_info *ndpi;

                            ndpi = xrealloc(*dpi, (*nr + 1) * sizeof(*ndpi));
                            if (!ndpi) {
                                hvfs_err(lib, "xrealloc disk_part_info failed.\n");
                                goto next_line;
                            }
                            *dpi = ndpi;
                            *nr = *nr + 1;
                        }
                        break;
                    case 2:
                        /* it is dev_sn */
                        (*dpi)[*nr - 1].dev_sn = strdup(p);
                        break;
                    case 3:
                        /* it is dev id */
                        (*dpi)[*nr - 1].dev_id = strdup(p);
                        break;
                    case 4:
                        /* it is mount path */
                        (*dpi)[*nr - 1].mount_path = strdup(p);
                        /* extract the remaining fields as mount path (handling spaces) */
                        if (p - line + strlen(p) + 2 < lnr) {
                            xfree((*dpi)[*nr - 1].mount_path);
                            p[strlen(p)] = ' ';
                            line[lnr - 1] = '\0';
                            (*dpi)[*nr - 1].mount_path = strdup(p);
                        }
                        (*dpi)[*nr - 1].read_nr = 0;
                        (*dpi)[*nr - 1].write_nr = 0;
                        (*dpi)[*nr - 1].err_nr = 0;
                        (*dpi)[*nr - 1].used = 0;
                        (*dpi)[*nr - 1].free = 0;
                        break;
                    }
                    hvfs_debug(lib, "2 %d %s FFF\n", i, p);
                }
                // do filtering
                if (*nr > 0) {
                    for (i = 0; i < g_ds_conf.sdfilter_len; i++) {
                        if (strstr((*dpi)[*nr - 1].dev_id, 
                                   g_ds_conf.sdfilter[i]) != NULL) {
                            // filtered
                            *nr -= 1;
                            filtered = 1;
                            break;
                        }
                    }
                    if (!filtered) {
                        for (i = 0; i < g_ds_conf.mpfilter_len; i++) {
                            if (strcmp((*dpi)[*nr - 1].mount_path, 
                                       g_ds_conf.mpfilter[i]) == 0) {
                                // filtered
                                *nr -= 1;
                                filtered = 1;
                                break;
                            }
                        }
                    }
                    if (filtered) {
                        // free any resouces
                        xfree((*dpi)[*nr].dev_sn);
                        xfree((*dpi)[*nr].dev_id);
                        xfree((*dpi)[*nr].mount_path);
                    }
                }
            }
        next_line:;
        }
        free(line);
    }
    pclose(f);

    if (*nr > 0) {
        /* gather stats */
        struct disk_part_info *t = *dpi;
        struct statfs s;
        int i;

        for (i = 0; i < *nr; i++) {
            /* get disk read/write sectors here */
            err = get_disk_stats(t[i].dev_id, &t[i].read_nr, &t[i].write_nr,
                                 &t[i].err_nr);
            if (err) {
                t[i].read_nr = 0;
                t[i].write_nr = 0;
                t[i].err_nr = 0;
            }

            err = statfs(t[i].mount_path, &s);
            if (err) {
                hvfs_err(lib, "statfs(%s) failed w/ %s\n",
                         t[i].mount_path, strerror(errno));
                /* ignore this dev, do not alloc new files on this dev */
                t[i].used = 0;
                t[i].free = 0;
                /* BUG-fix: ignore this error, only prevent new allocations */
                err = 0;
            } else {
                t[i].used = s.f_bsize * (s.f_blocks - s.f_bavail);
                t[i].free = s.f_bsize * s.f_bavail;
            }
        }
    }
out:    
    return err;
}

int get_disk_sn_ext(struct disk_part_info **dpi, int *nr, char *label)
{
    char buf[1024];
    char *line = NULL;
    size_t len = 0;
    FILE *f;
    int err = 0, lnr = 0;

    sprintf(buf, GET_GL_DISK_SN_EXT, label, label);
    f = popen(buf, "r");

    if (f == NULL) {
        err = -errno;
        hvfs_err(lib, "popen(GET_GL_DISKPART_SN) failed w/ %s\n",
                 strerror(errno));
        goto out;
    } else {
        /* read from the result stream */
        while ((lnr = getline(&line, &len, f)) != -1) {
            /* ok, parse the results */
            if (lnr > 2) {
                char *p, *sp;
                int i, filtered = 0;

                for (i = 0, p = line; ++i; p = NULL) {
                    p = strtok_r(p, " \n", &sp);
                    if (!p)
                        break;
                    hvfs_debug(lib, "2 %d %s\n", i, p);
                    switch (i) {
                    case 1:
                        /* check whether it is OK */
                        if (strncmp(p, "OK", 2) != 0) {
                            /* bad */
                            hvfs_err(lib, "BAD line w/ '%s', ignore it\n", p);
                            goto next_line;
                        }
                        /* alloc a new disk_part_info entry */
                        {
                            struct disk_part_info *ndpi;

                            ndpi = xrealloc(*dpi, (*nr + 1) * sizeof(*ndpi));
                            if (!ndpi) {
                                hvfs_err(lib, "xrealloc disk_part_info failed.\n");
                                goto next_line;
                            }
                            *dpi = ndpi;
                            *nr = *nr + 1;
                        }
                        break;
                    case 2:
                        /* it is dev_sn */
                        (*dpi)[*nr - 1].dev_sn = strdup(p);
                        break;
                    case 3:
                        /* it is dev id */
                        (*dpi)[*nr - 1].dev_id = strdup(p);
                        break;
                    case 4:
                        /* it is mount path */
                        (*dpi)[*nr - 1].mount_path = strdup(p);
                        /* extract the remaining fields as mount path (handling spaces) */
                        if (p - line + strlen(p) + 2 < lnr) {
                            xfree((*dpi)[*nr - 1].mount_path);
                            p[strlen(p)] = ' ';
                            line[lnr - 1] = '\0';
                            (*dpi)[*nr - 1].mount_path = strdup(p);
                        }
                        (*dpi)[*nr - 1].read_nr = 0;
                        (*dpi)[*nr - 1].write_nr = 0;
                        (*dpi)[*nr - 1].err_nr = 0;
                        (*dpi)[*nr - 1].used = 0;
                        (*dpi)[*nr - 1].free = 0;
                        break;
                    }
                    hvfs_debug(lib, "2 %d %s FFF\n", i, p);
                }
                // do filtering
                if (*nr > 0) {
                    for (i = 0; i < g_ds_conf.sdfilter_len; i++) {
                        if (strstr((*dpi)[*nr - 1].dev_id, 
                                   g_ds_conf.sdfilter[i]) != NULL) {
                            // filtered
                            *nr -= 1;
                            filtered = 1;
                            break;
                        }
                    }
                    if (!filtered) {
                        for (i = 0; i < g_ds_conf.mpfilter_len; i++) {
                            if (strcmp((*dpi)[*nr - 1].mount_path, 
                                       g_ds_conf.mpfilter[i]) == 0) {
                                // filtered
                                *nr -= 1;
                                filtered = 1;
                                break;
                            }
                        }
                    }
                    if (filtered) {
                        // free any resouces
                        xfree((*dpi)[*nr].dev_sn);
                        xfree((*dpi)[*nr].dev_id);
                        xfree((*dpi)[*nr].mount_path);
                    }
                }
            }
        next_line:;
        }
        free(line);
    }
    pclose(f);

    if (*nr > 0) {
        /* gather stats */
        struct disk_part_info *t = *dpi;
        struct statfs s;
        int i;

        for (i = 0; i < *nr; i++) {
            /* get disk read/write sectors here */
            err = get_disk_stats(t[i].dev_id, &t[i].read_nr, &t[i].write_nr,
                                 &t[i].err_nr);
            if (err) {
                t[i].read_nr = 0;
                t[i].write_nr = 0;
                t[i].err_nr = 0;
            }

            err = statfs(t[i].mount_path, &s);
            if (err) {
                hvfs_err(lib, "statfs(%s) failed w/ %s\n",
                         t[i].mount_path, strerror(errno));
                /* ignore this dev, do not alloc new files on this dev */
                t[i].used = 0;
                t[i].free = 0;
                /* BUG-fix: ignore this error, only prevent new allocations */
                err = 0;
            } else {
                t[i].used = s.f_bsize * (s.f_blocks - s.f_bavail);
                t[i].free = s.f_bsize * s.f_bavail;
            }
        }
    }
out:    
    return err;
}

int get_disk_parts(struct disk_part_info **dpi, int *nr, char *label)
{
    int err = 0;

    *dpi = NULL;
    *nr = 0;
    
    err = get_disk_parts_sn(dpi, nr, label);
    if (err) {
        goto out;
    }
    err = get_disk_sn_ext(dpi, nr, label);
    if (err) {
        goto out;
    }

out:
    return err;
}


void dpi_free(struct disk_part_info *dpi, int nr)
{
    int i;
    
    for (i = 0; i < nr; i++) {
        xfree(dpi[i].dev_sn);
        xfree(dpi[i].dev_id);
        xfree(dpi[i].mount_path);
    }
    
    xfree(dpi);
}

int open_shm(int flag)
{
    int fd = 0, oflag = O_RDWR | flag;

reopen:
    fd = shm_open(DEV_MAPPING, oflag, 0644);
    if (fd < 0) {
        if (errno == ENOENT) {
            oflag |= O_CREAT;
            hvfs_info(lib, "Try to create a new SHM object\n");
            goto reopen;
        }
        hvfs_err(lib, "shm_open(%s) failed w/ %s\n",
                 DEV_MAPPING, strerror(errno));
        return -1;
    }

    return fd;
}

int open_audit()
{
    int fd = 0, oflag = O_RDWR | O_CREAT;
    int err = 0;

    fd = shm_open(DS_AUDIT, oflag, 0644);
    if (fd < 0) {
        printf("shm_open(%s) failed w/ %s\n", DS_AUDIT, strerror(errno));
        return -1;
    }
    // BUG-XXX: should allow other user write this file
    err = fchmod(fd, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (err < 0) {
        printf("fchmod() failed w/ %s\n", strerror(errno));
        close(fd);
        return -1;
    }

    return fd;
}

#define SHMLOCK_UN      0
#define SHMLOCK_WR      1
#define SHMLOCK_RD      2
int lock_shm(int fd, int type)
{
    struct flock lock;
    int err = 0;

    switch (type) {
    case SHMLOCK_UN:
        lock.l_type = F_UNLCK;
        break;
    case SHMLOCK_WR:
        lock.l_type = F_WRLCK;
        break;
    case SHMLOCK_RD:
        lock.l_type = F_RDLCK;
        break;
    }
    lock.l_start = 0;
    lock.l_whence = SEEK_SET;
    lock.l_len = 0;
    lock.l_pid = getpid();

    err = fcntl(fd, F_SETLKW, &lock);
    if (err) {
        hvfs_err(lib, "lock shm file failed w/ %s\n",
                 strerror(errno));
    }

    return err;
}

int read_shm(int fd, struct disk_part_info *dpi, int nr)
{
    struct disk_part_info this;
    char buf[64 * 1024];
    char *p, *s1, *s2, *init, *line;
    int br, bl = 0, err = 0, i = 0;

    do {
        br = read(fd, buf + bl, 64 * 1024);
        if (br < 0) {
            hvfs_err(lib, "read() failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        } else if (br == 0) {
            /* ok */
            break;
        }
        bl += br;
    } while (bl < 64 * 1024);

    init = buf;
    do {
        p = strtok_r(init, "\n", &s1);
        if (!p) {
            hvfs_debug(lib, "No avaliable line\n");
            break;
        }

        line = strdup(p);
        hvfs_debug(lib, "LINE: %s\n", line);
        p = strtok_r(line, ":", &s2);
        if (!p) {
            hvfs_debug(lib, "key: %s\n", p);
            continue;
        }
        memset(&this, 0, sizeof(this));
        this.dev_sn = strdup(p);
        while (1) {
            p = strtok_r(NULL, ",", &s2);
            if (!p) {
                hvfs_debug(lib, "v=%s\n", p);
                break;
            }
            i++;
            switch (i) {
            case 1:
                hvfs_debug(lib, "%s\n", p);
                this.mount_path = strdup(p);
                break;
            case 2:
                hvfs_debug(lib, "%s\n", p);
                this.read_nr = atol(p);
                break;
             case 3:
                hvfs_debug(lib, "%s\n", p);
                this.write_nr = atol(p);
                break;
            case 4:
                hvfs_debug(lib, "%s\n", p);
                this.err_nr = atol(p);
            default:;
            }
        }
        /* BUG-XXX: do NOT update nrs from SHM file; otherwise we might
         * corrupt /sys/block/xxx/stat NRs. */
        /* find and update */
        /*for (i = 0; i < nr; i++) {
            if (strcmp(dpi[i].dev_sn, this.dev_sn) == 0) {
                dpi[i].read_nr = this.read_nr;
                dpi[i].write_nr = this.write_nr;
                dpi[i].err_nr = this.err_nr;
                break;
            }
        }
        */
        xfree(this.mount_path);
        xfree(this.dev_sn);
        xfree(line);
    } while ((init = NULL), 1);

out:
    return err;
}

int write_shm(int fd, struct disk_part_info *dpi, int nr)
{
    char buf[64 * 1024];
    int i, bw, bl = 0, err = 0, n = 0;

    memset(buf, 0, sizeof(buf));
    if (!g_specify_dev) {
        for (i = 0; i < nr; i++) {
            n += sprintf(buf + n, "%s:%s,%ld,%ld,%ld\n", 
                         dpi[i].dev_sn, dpi[i].mount_path,
                         dpi[i].read_nr, dpi[i].write_nr, dpi[i].err_nr);
        }
    } else {
        sprintf(buf, "%s,0,0,0\n", g_dev_str);
    }

    do {
        bw = write(fd, buf + bl, strlen(buf) - bl);
        if (bw < 0) {
            hvfs_err(lib, "write() failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        }
        bl += bw;
    } while (bl < strlen(buf));

    /* BUG-XXX: we have truncate the fd to actual length */
    err = ftruncate(fd, bl);
    if (err) {
        hvfs_err(lib, "ftruncate() failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

    err = bl;
    hvfs_info(lib, "WRITE SHM: {%s}\n", buf);

out:
    return err;
}

void unlink_shm()
{
    shm_unlink(DEV_MAPPING);
}

/* Args
 * @server: server name
 * @port: server port
 */
int setup_dgram(char *server, int port)
{
    struct timeval timeout = {30, 0};
    struct hostent* hostInfo;
    int err = 0, ttl = 64;
    
    g_sockfd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (g_sockfd == -1) {
        hvfs_err(lib, "socket() failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }
    if ((setsockopt(g_sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, 
                    sizeof(timeout))) != 0) {
        hvfs_err(lib, "setsockopt() failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

    if ((setsockopt(g_sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, 
                    sizeof(timeout))) != 0) {
        hvfs_err(lib, "setsockopt() failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

    /* BUG-XXX: for larget system, we have to add ttl to route multicast IP
     * packets in several subnets. */
    if ((setsockopt(g_sockfd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, 
                    sizeof(ttl))) != 0) {
        hvfs_err(lib, "setsockopt() failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

    g_server.sin_family = AF_INET;
    g_server.sin_port = htons(port);
    hostInfo = gethostbyname(server);

    if (hostInfo == NULL) {
        hvfs_err(lib, "getnostbyname(%s) failed w/ %s\n", server, 
                 strerror(errno));
        goto out;
    }

    g_server.sin_addr = *(struct in_addr*) hostInfo->h_addr_list[0];

    hvfs_info(lib, "Setup UDP socket for DiskManager @ %s:%d\n",
              g_server_str, g_port);

out:
    return err;
}

/* Args
 * @arg1: send => data to send
 * @arg2: recv => data buffer to recv
 */
int __dgram_sr(char *send, char **recv)
{
    socklen_t serverSize = sizeof(g_serverR);
    char reply[1500];
    int br, gotResponse = 0, err = 0;

    if ((sendto(g_sockfd, send, strlen(send), 0, (struct sockaddr*) &g_server,
                sizeof(g_server))) == -1) {
        hvfs_err(lib, "Send datagram packet failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }
    
    memset(reply, 0, sizeof(reply));
    if ((br = recvfrom(g_sockfd, reply, 1500, 0,
                       (struct sockaddr*) &g_serverR, &serverSize)) == -1) {
        if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
            hvfs_err(lib, "Recv TIMEOUT!\n");
            err = -errno;
        } else {
            hvfs_err(lib, "Recvfrom HB reply failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        }
    } else
        gotResponse = 1;
        
    if (gotResponse) {
        hvfs_info(lib, "recv %s\n", reply);
        *recv = strdup(reply);
    } else
        *recv = NULL;

out:
    return err;
}

/* Args
 * @arg1: send => data to send
 * @arg2: recv => data buffer to recv
 */
static inline int __dgram_send(char *send)
{
    int err = 0;

    if ((sendto(g_sockfd, send, strlen(send), 0, (struct sockaddr*) &g_server,
                sizeof(g_server))) == -1) {
        hvfs_err(lib, "Send datagram packet failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }
    
out:
    return err;
}

int __dgram_sendmsg(struct iovec *iov, int iov_len)
{
    ssize_t err = 0;
    struct msghdr msg = {
        .msg_name = (void *)&g_server,
        .msg_namelen = sizeof(g_server),
        .msg_iov = iov,
        .msg_iovlen = iov_len,
    };

    err = sendmsg(g_sockfd, &msg, MSG_NOSIGNAL);
    if (err < 0) {
        hvfs_err(lib, "Send datagram packet (iov) failed w/ %s\n",
                 strerror(errno));
        err = -errno;
        goto out;
    }
out:
    return err;
}

int fix_disk_parts(struct disk_part_info *dpi, int nr)
{
    int fd = open_shm(0);
    int err = 0;

    if (fd > 0) {
        lock_shm(fd, SHMLOCK_RD);
        err = read_shm(fd, dpi, nr);
        lock_shm(fd, SHMLOCK_UN);
    } else
        err = -errno;
    close(fd);
    
    return err;
}

#define DTRACE_REP      0
#define DTRACE_REP_ERR  1
#define DTRACE_DEL      2
#define DTRACE_DEL_ERR  3

void update_dtrace(char *dev_sn, int type, int latency)
{
    struct dtrace_args *pos;
    int found = 0;

    xlock_lock(&g_dtrace_lock);
retry:
    list_for_each_entry(pos, &g_dtrace, list) {
        if (strcmp(pos->dev_sn, dev_sn) == 0) {
            switch (type) {
            case DTRACE_REP:
                pos->rep_nr++;
                pos->rep_lat += latency;
                break;
            case DTRACE_REP_ERR:
                pos->rep_err++;
                break;
            case DTRACE_DEL:
                pos->del_nr++;
                pos->del_lat += latency;
                break;
            case DTRACE_DEL_ERR:
                pos->del_err++;
                break;
            }
            found = 1;
            break;
        }
    }
    if (!found) {
        struct dtrace_args *da = xzalloc(sizeof(struct dtrace_args));

        if (da) {
            INIT_LIST_HEAD(&da->list);
            da->dev_sn = strdup(dev_sn);
            da->born = time(NULL);
            /* insert into table */
            list_add_tail(&da->list, &g_dtrace);
            goto retry;
        }
    }
    xlock_unlock(&g_dtrace_lock);
    if (found && !g_dtrace_update)
        g_dtrace_update = 1;
}

void check_dtrace()
{
    struct dtrace_args *pos;
    char p[1024], *q;

    if (g_dtrace_update) {
        q = p;
        xlock_lock(&g_dtrace_lock);
        list_for_each_entry(pos, &g_dtrace, list) {
            q += snprintf(q, 1023, "+B:%s,%ld,%ld,%ld,%ld,%ld,%ld\n",
                          pos->dev_sn,
                          pos->rep_nr, pos->rep_err, pos->rep_lat,
                          pos->del_nr, pos->del_err, pos->del_lat);
            hvfs_info(lib, "DEV:%s REP(%ld,%ld,%lf), DEL(%ld,%ld,%lf)\n",
                      pos->dev_sn,
                      pos->rep_nr, pos->rep_err,
                      ((double)pos->rep_lat / pos->rep_nr),
                      pos->del_nr, pos->del_err,
                      ((double)pos->del_lat / pos->del_nr));
        }
        xlock_unlock(&g_dtrace_lock);
        g_dtrace_update = 0;
        /* send the +B: info to server */
        if (q - p > 0) {
            struct iovec iov[2];

            iov[0].iov_base = g_audit_header;
            iov[0].iov_len = strlen(g_audit_header);
            iov[1].iov_base = p;
            iov[1].iov_len = q - p;
            __dgram_sendmsg(iov, 2);
        }
    }
}

void refresh_map(time_t cur)
{
    static time_t last = 0;
    struct disk_part_info *dpi = NULL;
    int nr = 0, fd, err, update = 0;

    /* map refresh interval */
    if (cur - last > g_ds_conf.mr_interval) {
        err = get_disk_parts(&dpi, &nr, g_devtype == NULL ? "scsi" : g_devtype);
        if (err) {
            hvfs_err(lib, "get_disk_parts() failed w/ %d\n", err);
            goto update_last;
        }
        
        err = fix_disk_parts(dpi, nr);
        if (err) {
            hvfs_warning(lib, "fix_disk_parts() failed w/ %s(%d), ignore "
                     "any NRs\n", strerror(-err), err);
        }
        fd = open_shm(0);
        lock_shm(fd, SHMLOCK_WR);
        write_shm(fd, dpi, nr);
        lock_shm(fd, SHMLOCK_UN);
        close(fd);

        xlock_lock(&g_dpi_lock);
        if (g_dpi) {
            dpi_free(g_dpi, g_nr);
            update = 1;
        }
        g_dpi = dpi;
        g_nr = nr;
        xlock_unlock(&g_dpi_lock);

        hvfs_info(lib, "Map refreshed and g_dpi updated=%d\n", update);
    update_last:
        last = cur;
    } else if (cur - last < 0) {
        hvfs_warning(lib, "Detect timestamp adjust backward, reset lastTS.\n");
        last = cur;
    }
}

char *getaline(char *buf, int *off)
{
    char *r,*p;

    r = p = buf + *off;

    while (1) {
        switch (*p) {
        case '\0':
            goto out;
            break;
        case '\n':
            *p = '\0';
            p++;
            goto out;
            break;
        default:;
        }
        p++;
    }
out:
    *off += p - r;

    if (r == p)
        return NULL;
    else
        return r;
}

#define CHECK_TOKEN_SIZE(token, t, s) if (token.type != t || token.size != s) continue;
#define CHECK_TOKEN_CONT(token, t, pos) ({                              \
            if (token.type != t) continue;                              \
            memset(pos, 0, sizeof(pos));                                \
            memcpy(pos, line + 5 + token.start, token.end - token.start); \
        })
#define GET_TOK(token, tok) ({                                          \
            memset(tok, 0, sizeof(tok));                                \
            memcpy(tok, line + 5 + token.start, token.end - token.start);   \
        })

#define SET_RA_TO(begin, end) do {                          \
        for (i = begin; i < end; i+=2) {                    \
            GET_TOK(tokens[i], tok);                        \
            if (strcmp(tok, "mp") == 0) {                   \
                GET_TOK(tokens[i + 1], tok);                \
                ra->to.mp = strdup(tok);                    \
            } else if (strcmp(tok, "devid") == 0) {         \
                GET_TOK(tokens[i + 1], tok);                \
                ra->to.devid = strdup(tok);                 \
            } else if (strcmp(tok, "location") == 0) {      \
                GET_TOK(tokens[i + 1], tok);                \
                ra->to.location = strdup(tok);              \
            } else if (strcmp(tok, "node_name") == 0) {     \
                GET_TOK(tokens[i + 1], tok);                \
                ra->to.node = strdup(tok);                  \
            }                                               \
        }                                                   \
    } while (0)

#define SET_RA_FROM(begin, end) do {                          \
        for (i = begin; i < end; i+=2) {                      \
            GET_TOK(tokens[i], tok);                          \
            if (strcmp(tok, "mp") == 0) {                     \
                GET_TOK(tokens[i + 1], tok);                  \
                ra->from.mp = strdup(tok);                    \
            } else if (strcmp(tok, "devid") == 0) {           \
                GET_TOK(tokens[i + 1], tok);                  \
                ra->from.devid = strdup(tok);                 \
            } else if (strcmp(tok, "location") == 0) {        \
                GET_TOK(tokens[i + 1], tok);                  \
                ra->from.location = strdup(tok);              \
            } else if (strcmp(tok, "node_name") == 0) {       \
                GET_TOK(tokens[i + 1], tok);                  \
                ra->from.node = strdup(tok);                  \
            }                                                 \
        }                                                     \
    } while (0)


int handle_commands(char *recv)
{
    char *line, tok[4096];
    int off = 0, r;
    jsmn_parser p;
    jsmntok_t tokens[30];
    int has_cmds = 0;
    
    while ((line = getaline(recv, &off)) != NULL) {
        hvfs_info(lib, "LINE: %s\n", line);
        if (strncmp(line, "+REP:", 5) == 0) {
            struct rep_args *ra;
            char pos[32];
            int i;
            
            jsmn_init(&p);
            r = jsmn_parse(&p, line + 5, tokens, 30);
            if (r != JSMN_SUCCESS) {
                hvfs_err(lib, "parse JSON string '%s' failed w/ %d\n", 
                         line, r);
                continue;
            }

            ra = xzalloc(sizeof(*ra));
            if (!ra) {
                hvfs_err(lib, "xzalloc() rep_args failed\n");
                continue;
            }
            ra->ttl = time(NULL);
            
            /* parse the json object */
            CHECK_TOKEN_SIZE(tokens[0], JSMN_OBJECT, 4);
            CHECK_TOKEN_CONT(tokens[1], JSMN_STRING, pos);
            CHECK_TOKEN_SIZE(tokens[2], JSMN_OBJECT, 8);

            if (strcmp(pos, "to") == 0)
                SET_RA_TO(3, 11);
            else if (strcmp(pos, "from") == 0)
                SET_RA_FROM(3, 11);

            CHECK_TOKEN_CONT(tokens[11], JSMN_STRING, pos);
            CHECK_TOKEN_SIZE(tokens[12], JSMN_OBJECT, 8);

            if (strcmp(pos, "from") == 0)
                SET_RA_FROM(13, 21);
            else if (strcmp(pos, "to") == 0)
                SET_RA_TO(13, 21);

            /* convert hostname to ip address */
            {
                char *__t;
                char ip[NI_MAXHOST];

                __t = ra->from.node;
                __convert_host_to_ip(__t, ip);
                ra->from.node = strdup(ip);
                xfree(__t);

                __t = ra->to.node;
                __convert_host_to_ip(__t, ip);
                ra->to.node = strdup(ip);
                xfree(__t);
            }

            xlock_lock(&g_rep_lock);
            list_add_tail(&ra->list, &g_rep);
            xlock_unlock(&g_rep_lock);
            sem_post(&g_rep_sem);
            atomic64_inc(&g_di.qrep);

            has_cmds = 1;
        } else if (strncmp(line, "+DEL:", 5) == 0) {
            struct del_args *ra;
            char *p = line + 5, *sp;
            int i;

            ra = xzalloc(sizeof(*ra));
            if (!ra) {
                hvfs_err(lib, "xzalloc() del_args failed\n");
                continue;
            }
            ra->ttl = time(NULL);

            /* parse the : seperated string */
            for (i = 0; ++i; p = NULL) {
                p = strtok_r(p, ":", &sp);
                if (!p)
                    break;
                hvfs_info(lib, "GOT %d %s\n", i, p);
                switch (i) {
                case 1:
                    /* node_name */
                    ra->target.node = strdup(p);
                    break;
                case 2:
                    /* devid */
                    ra->target.devid = strdup(p);
                    break;
                case 3:
                    /* mp */
                    ra->target.mp = strdup(p);
                    break;
                case 4:
                    /* location */
                    ra->target.location = strdup(p);
                    break;
                }
            }

            xlock_lock(&g_del_lock);
            list_add_tail(&ra->list, &g_del);
            xlock_unlock(&g_del_lock);
            sem_post(&g_del_sem);
            atomic64_inc(&g_di.qdel);

            has_cmds = 1;
        } else if (strncmp(line, "+VYR:", 5) == 0) {
            /* this means we should check on these files. If they existed too
             * long (10 days), delete it now */
            char *p = line + 5, *sp;
            char fpath[PATH_MAX];
            struct stat buf;
            int i, len = 0, err;

            /* parse the : seperated string */
            for (i = 0; ++i; p = NULL) {
                p = strtok_r(p, ":", &sp);
                if (!p)
                    break;
                hvfs_info(lib, "GOT %d %s\n", i, p);
                switch (i) {
                case 1:
                    /* devid */
                    break;
                case 2:
                    /* mp */
                    len += sprintf(fpath + len, "%s/", p);
                    break;
                case 3:
                    /* location */
                    len += sprintf(fpath + len, "%s", p);
                    break;
                }
            }
            atomic64_inc(&g_di.tvyr);
            atomic_set(&g_di.updated, 1);
            err = stat(fpath, &buf);
            if (err) {
                hvfs_err(lib, "stat(%s) failed w/ %s(%d)\n",
                         fpath, strerror(errno), errno);
            } else {
                if (time(NULL) - buf.st_mtime > 
                    g_ds_conf.max_keeping_days * 86400) {
                    char cmd[8192];
                    FILE *f;

                    hvfs_info(lib, "Verify '%s' time range: [%ld days], del it\n",
                              fpath, (time(NULL) - buf.st_mtime) / 86400);
                    sprintf(cmd, "%s rm -rf %s", g_timeout_str, fpath);
                    f = popen(cmd, "r");
                    if (f == NULL) {
                        hvfs_err(lib, "popen(%s) failed w/ %s\n",
                                 cmd, strerror(errno));
                    } else {
                        pclose(f);
                    }
                }
            }
        }
    }

    return has_cmds;
}

static void __free_rep_args(struct rep_args *ra)
{
    xfree(ra->from.node);
    xfree(ra->from.mp);
    xfree(ra->from.devid);
    xfree(ra->from.location);
    xfree(ra->to.node);
    xfree(ra->to.mp);
    xfree(ra->to.devid);
    xfree(ra->to.location);
}

static void __free_del_args(struct del_args *da)
{
    xfree(da->target.node);
    xfree(da->target.mp);
    xfree(da->target.devid);
    xfree(da->target.location);
}

static void __free_verify_args(struct verify_args *va)
{
    xfree(va->target.node);
    xfree(va->target.mp);
    xfree(va->target.devid);
    xfree(va->target.location);
}

void __check_and_drop_reps()
{
    struct rep_args *pos, *n;

    xlock_lock(&g_rep_lock);
    list_for_each_entry_safe(pos, n, &g_rep, list) {
        if (pos->status == REP_STATE_INIT && 
            pos->ttl + g_ds_conf.fl_interval < time(NULL)) {
            hvfs_info(lib, "DROP REP TO{%s:%s/%s} FROM{%s:%s/%s} status %d\n",
                      pos->to.node, pos->to.mp, pos->to.location,
                      pos->from.node, pos->from.mp, pos->from.location,
                      pos->status);
            pos->status = REP_STATE_ERROR_DONE;
            pos->errcode = -ETIMEDOUT;
        }
    }
    xlock_unlock(&g_rep_lock);
}

int __do_heartbeat()
{
    char query[64 * 1024];
    struct rep_args *pos, *n;
    struct del_args *pos2, *n2;
    struct verify_args *pos3, *n3;
    int err = 0, nr2 = 0, nr_max = 10;

    int i, len = 0, mode = NORMAL_HB_MODE;
    
    memset(query, 0, sizeof(query));
    len += sprintf(query, "+node:%s\n", g_hostname);
    
    if (!g_specify_dev) {
        /* Using global dpi */
        xlock_lock(&g_dpi_lock);
        for (i = 0; i < g_nr; i++) {
            len += sprintf(query + len, "%s:%s,%ld,%ld,%ld,%ld,%ld\n",
                           g_dpi[i].dev_sn, g_dpi[i].mount_path,
                           g_dpi[i].read_nr, g_dpi[i].write_nr, g_dpi[i].err_nr,
                           g_dpi[i].used, g_dpi[i].free);
        }
        xlock_unlock(&g_dpi_lock);
    } else {
        char str[512];
        
        sprintf(str, "%s,0,0,0,0,%ld\n", g_dev_str, LONG_MAX);
        len += sprintf(query + len, "%s", str);
    }

    if (len == 0)
        goto out;

#if 1
    
    /* TODO: append any commands to report */
    len += sprintf(query + len, "+CMD\n");
    /* Add new INFO cmd: collect this node's load, queued rep/del */
    if (__get_sysinfo()) {
        len += sprintf(query + len, "+INFO:%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,"
                       "%ld,%0.2f,%ld\n",
                       atomic64_read(&g_di.qrep),
                       atomic64_read(&g_di.hrep),
                       atomic64_read(&g_di.drep),
                       atomic64_read(&g_di.qdel),
                       atomic64_read(&g_di.hdel),
                       atomic64_read(&g_di.ddel),
                       atomic64_read(&g_di.tver),
                       atomic64_read(&g_di.tvyr),
                       g_di.uptime,
                       g_di.loadavg[0],
                       (time(NULL) - g_di.recvts)
            );
    }

    xlock_lock(&g_rep_lock);
    list_for_each_entry_safe(pos, n, &g_rep, list) {
        if (nr2 >= nr_max) {
            mode = QUICK_HB_MODE;
            break;
        }
        hvfs_info(lib, "POS REP_R to.location %s status %d latency=%ld\n",
                  pos->to.location, pos->status,
                  (pos->status == REP_STATE_DONE ? pos->latency : -1L));
        if (pos->status == REP_STATE_DONE) {
            /* update dtrace info */
            update_dtrace(pos->to.devid, DTRACE_REP, pos->latency);

            len += sprintf(query + len, "+REP:%s,%s,%s,%s,%ld\n",
                           pos->to.node, pos->to.devid, pos->to.location,
                           pos->digest,
                           pos->latency);
            list_del(&pos->list);
            __free_rep_args(pos);
            xfree(pos);
            atomic64_dec(&g_di.qrep);
            atomic64_dec(&g_di.drep);
            nr2++;
        } else if (pos->status == REP_STATE_ERROR_DONE) {
            /* update dtrace info */
            update_dtrace(pos->to.devid, DTRACE_REP_ERR, pos->latency);

            len += sprintf(query + len, "+FAIL:REP:%s,%s,%s,%d\n",
                           pos->to.node, pos->to.devid, pos->to.location,
                           pos->errcode);
            list_del(&pos->list);
            __free_rep_args(pos);
            xfree(pos);
            atomic64_dec(&g_di.qrep);
            atomic64_dec(&g_di.drep);
            nr2++;
        }
    }
    xlock_unlock(&g_rep_lock);

    //__check_and_drop_reps();
    
    xlock_lock(&g_del_lock);
    list_for_each_entry_safe(pos2, n2, &g_del, list) {
        if (nr2 >= nr_max) {
            mode = QUICK_HB_MODE;
            break;
        }
        hvfs_info(lib, "POS DEL_R target.location %s status %d latency=%ld\n",
                  pos2->target.location, pos2->status,
                  (pos2->status == REP_STATE_DONE ? pos2->latency : -1L));
        if (pos2->status == DEL_STATE_DONE) {
            /* update dtrace info */
            update_dtrace(pos2->target.devid, DTRACE_DEL, pos2->latency);

            len += sprintf(query + len, "+DEL:%s,%s,%s,%ld\n",
                           pos2->target.node, pos2->target.devid, 
                           pos2->target.location,
                           pos2->latency);
            list_del(&pos2->list);
            __free_del_args(pos2);
            xfree(pos2);
            atomic64_dec(&g_di.qdel);
            atomic64_dec(&g_di.ddel);
            nr2++;
        } else if (pos2->status == DEL_STATE_ERROR_DONE) {
            /* update dtrace info */
            update_dtrace(pos2->target.devid, DTRACE_DEL_ERR, pos2->latency);

            len += sprintf(query + len, "+FAIL:DEL:%s,%s,%s,%d\n",
                           pos2->target.node, pos2->target.devid, 
                           pos2->target.location,
                           pos2->errcode);
            list_del(&pos2->list);
            __free_del_args(pos2);
            xfree(pos2);
            atomic64_dec(&g_di.qdel);
            atomic64_dec(&g_di.ddel);
            nr2++;
        }
    }
    xlock_unlock(&g_del_lock);

    xlock_lock(&g_verify_lock);
    list_for_each_entry_safe(pos3, n3, &g_verify, list) {
        if (nr2 >= nr_max) {
            mode = QUICK_HB_MODE2;
            break;
        }
        hvfs_info(lib, "POS VERIFY dev %s loc %s lvl %d\n",
                  pos3->target.devid, pos3->target.location, pos3->level);
        len += sprintf(query + len, "+VERIFY:%s,%s,%s,%d\n",
                       g_hostname, pos3->target.devid,
                       pos3->target.location, pos3->level);
        list_del(&pos3->list);
        __free_verify_args(pos3);
        xfree(pos3);
        atomic64_inc(&g_di.tver);
        nr2++;
    }
    xlock_unlock(&g_verify_lock);
    
    err = __dgram_send(query);
    if (err) {
        hvfs_err(lib, "datagram send/recv failed w/ %s\n", 
                 strerror(-err));
    }
    
#else
    hvfs_plain(lib, "%s", query);
#endif

out:;
    return mode;
}

void do_heartbeat(time_t cur)
{
    static time_t last = 0;
    static int hb_interval = 0;

    if (cur - last >= hb_interval) {
        switch (__do_heartbeat()) {
        case QUICK_HB_MODE:
            hb_interval = QUICK_HB_MODE; /* reset to 1 second */
            break;
        case QUICK_HB_MODE2:
            hb_interval = QUICK_HB_MODE2; /* reset to 10 seconds */
            break;
        default:
            hb_interval = g_ds_conf.hb_interval;
        }
        last = cur;
    } else if (cur - last < 0) {
        hvfs_warning(lib, "Detect timestamp adjust backward, reset last HBTS.\n");
        last = cur;
    }
}

static inline void __post_ticks()
{
    int i = 0;
    
    for (i = 0; i < g_rep_thread_nr; i++) {
        atomic64_inc(&g_rep_thread_ticks[i]);
    }
    for (i = 0; i < g_del_thread_nr; i++) {
        atomic64_inc(&g_del_thread_ticks[i]);
    }
}

static void __itimer_default(int signo, siginfo_t *info, void *arg)
{
    sem_post(&g_timer_sem);
    __post_ticks();
    hvfs_verbose(lib, "Did this signal handler called?\n");

    return;
}

static void __do_kill(char *tag, int pid)
{
    char cmd[256];
    FILE *f = NULL;

    sprintf(cmd, "kill %d", pid);
    f = popen(cmd, "r");
    if (f == NULL) {
        hvfs_err(lib, "popen(%s) failed w/ %s\n",
                 cmd, strerror(errno));
    } else {
        hvfs_err(lib, "Try to kill child %s process %d\n",
                 tag, pid);
        pclose(f);
    }
}

static int check_thread_liveness()
{
    int i;

    for (i = 0; i < g_rep_thread_nr; i++) {
        if (atomic64_read(&g_rep_thread_ticks[i]) > 
            g_ds_conf.rep_max_ticks) {
            /* clean for all running child processes */
            char cmd[4096];
            FILE *f = NULL;
            
            sprintf(cmd, "ps -ef | grep scp | grep -v grep | awk '$3==%u {print $2}'", 
                    getpid());
            f = popen(cmd, "r");

            if (f == NULL) {
                hvfs_err(lib, "popen(%s) failed w/ %s\n",
                         cmd, strerror(errno));
                continue;
            } else {
                char *line = NULL;
                size_t len = 0;
                
                while ((getline(&line, &len, f)) != -1) {
                    __do_kill("REP", atoi(line));
                }
                xfree(line);
            }
            pclose(f);
        }
        hvfs_debug(lib, "REP[%d] ticks=%ld\n", i, 
                   atomic64_read(&g_rep_thread_ticks[i]));
    }

    for (i = 0; i < g_del_thread_nr; i++) {
        if (atomic64_read(&g_del_thread_ticks[i]) > 
            g_ds_conf.del_max_ticks) {
            /* clean for all running child processes */
            char cmd[4096];
            FILE *f = NULL;
            
            sprintf(cmd, "ps -ef | grep rm | grep -v grep | awk '$3==%u {print $2}'", 
                    getpid());
            f = popen(cmd, "r");

            if (f == NULL) {
                hvfs_err(lib, "popen(%s) failed w/ %s\n",
                         cmd, strerror(errno));
                continue;
            } else {
                char *line = NULL;
                size_t len = 0;
                
                while ((getline(&line, &len, f)) != -1) {
                    __do_kill("DEL", atoi(line));
                }
                xfree(line);
            }
            pclose(f);
        }
        hvfs_debug(lib, "DEL[%d] ticks=%ld\n", i, 
                   atomic64_read(&g_del_thread_ticks[i]));
    }

    return 0;
}

static void *__timer_thread_main(void *arg)
{
    sigset_t set;
    time_t cur;
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
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        cur = time(NULL);
        /* first, refresh the map, then do report */
        refresh_map(cur);
        do_heartbeat(cur);
        /* trigger incomplete requests if they exists */
        sem_post(&g_rep_sem);
        sem_post(&g_del_sem);
        sem_post(&g_dscan_sem);
        sem_post(&g_audit_sem);
        /* ticks checking */
        sem_post(&g_liveness_sem);
        /* dtrace checking */
        check_dtrace();
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int setup_timers(int interval)
{
    struct sigaction ac;
    struct itimerval value, ovalue, pvalue;
    int which = ITIMER_REAL;
    int err;

    sem_init(&g_timer_sem, 0, 0);

    err = pthread_create(&g_timer_thread, NULL, &__timer_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create timer thread failed w/ %s\n", strerror(errno));
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
        hvfs_debug(lib, "OK, we have created a timer thread to "
                   " do scans every %d second(s).\n", interval);
    } else {
        hvfs_debug(lib, "Hoo, there is no need to setup itimers based on the"
                   " configration.\n");
        g_timer_thread_stop = 1;
    }
    
out:
    return err;
}

void do_help()
{
    hvfs_plain(lib,
               "Version 1.0.0e\n"
               "Copyright (c) 2013-2015 IIE and Ma Can <ml.macana@gmail.com>\n\n"
               "Arguments:\n"
               "-r, --server      DiskManager server IP address.\n"
               "-p, --port        UDP port of DiskManager.\n"
               "-t, --host        Set local host name.\n"
               "-d, --dev         Use this specified dev: DEVID:MOUNTPOINT.\n"
               "-f, --sdfilter    Filter for device name.\n"
               "-m, --mpfilter    Filter for mount point.\n"
               "-x, --mkdirs      Try to create the non-exist directory to REP success.\n"
               "-T, --devtype     Specify the disk type: e.g. scsi, ata, usb, ...\n"
               "-o, --timeo       Timeout value for reprot.\n"
               "-O, --thr_timeo   Thread Rep/Del timeout value.\n"
               "-S, --dscan       Enable device scanning.(default disabled)\n"
               "-A, --audit       Enable audit logging.(default disabled)\n"
               "-K, --tickR       Set tick for replicate thread\n"
               "-k, --tickD       Set tick for delete thread\n"
               "-b, --prob        Scan probility for single dir.\n"
               "-R, --reptn       Set rep thread number.\n"
               "-D, --deltn       Set del thread number.\n"
               "-I, --iph         IP addres hint to translate hostname to IP addr.\n"
               "-M, --mkd         Set max keeping days for lingering dirs.\n"
               "-C, --cpcmd       Copy command: rsync or scp.\n"
               "-h, -?, -help     print this help.\n"
        );
}

static void *__async_recv_thread_main(void *args)
{
    sigset_t set;
    socklen_t serverSize = sizeof(g_serverR);
    int br, err = 0;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we wait for incoming reply */
    while (!g_async_recv_thread_stop) {
        err = sem_wait(&g_async_recv_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }
        char reply[64 * 1024];

        while (!g_async_recv_thread_stop) {
            memset(reply, 0, sizeof(reply));
            if ((br = recvfrom(g_sockfd, reply, sizeof(reply), 0,
                               (struct sockaddr*) &g_serverR, &serverSize)) == -1) {
                if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                    hvfs_err(lib, "Recv TIMEOUT!\n");
                } else {
                    hvfs_err(lib, "Recv from Server reply failed w/ %s\n", strerror(errno));
                }
            } else {
                g_di.recvts = time(NULL);
                if (br == 0) {
                    /* target ordered shutdown? (in UDP?) */
                    hvfs_err(lib, "Recv from Server failed? w/ %s\n", strerror(errno));
                } else {
                    /* ok, get the reply */
                    if (strncmp(reply, "+OK", 3) != 0) {
                        hvfs_err(lib, "Invalid reply (or request) from server: %s(%d)\n",
                                 reply, br);
                    } else {
                        /* handle any piggyback commands */
                        if (handle_commands(reply))
                            __do_heartbeat();
                    }
                }
            }
        }
    }
    pthread_exit(0);
}

int remove_from_hdfs(struct del_args *pos)
{
    FILE *f;
    FILE *f2;
    char cmd[9216];
    char hdfs_dir_cmd[9216];
    char *hdfs_root_dir = NULL;

    char *jar_path = "/home/metastore/sotstore/dservice/lib/data_trans.jar";
    hvfs_info(lib,"libing:debug:step1:go into remove_from_hdfs");
    sprintf(hdfs_dir_cmd,"java -jar %s 0",jar_path);
    hvfs_info(lib,"libing:debug:step2:hdfs_dir_cmd:%s",hdfs_dir_cmd);

    f = popen(hdfs_dir_cmd, "r");
    if(f == NULL)
        {
            hvfs_info(lib,"popen(%s) failedw/\n",hdfs_dir_cmd);
        }else{
            char *line = NULL;
            size_t len = 0;
            while((getline(&line,&len,f)) != -1)
            {
                hvfs_info(lib,"libing:debug:step3.2:EXEC(%s):%s",hdfs_dir_cmd,line);
                hdfs_root_dir = line;
                hvfs_info(lib,"libing:debug:step3.3 hdfs_root_dir:%s",hdfs_root_dir);
            }

        }
    sprintf(cmd,"java -jar %s 4 %s%s",jar_path,hdfs_root_dir,pos->target.location);
    hvfs_info(lib,"libing:debug:step4:upload cmd is:%s",cmd);
        
    f2 = popen(cmd,"r");
    if(f2 == NULL)
    {
      hvfs_info(lib,"popen(%s) failed\n",cmd);
    }else{
      char *line = NULL;
      size_t len =0;
      while((getline(&line,&len,f2)) != -1){
     	 hvfs_info(lib,"libing:debug :step4.3:EXEC(%s):%s",cmd,line);
      }
     }
     int result = pclose(f2);
     if (-1 == result)
     {
	hvfs_info(lib,"pclose(%s) failedw/\n",cmd);
	exit(1);
      }
	else
      {
	if (WIFEXITED(result))
	{
	hvfs_info(lib, "CMD(%s) exited, status=%d\n", cmd, WEXITSTATUS(result));
	}
     }
     return 0;
}

static void *__del_thread_main(void *args)
{
    struct thread_args *ta = (struct thread_args *)args;
    sigset_t set;
    time_t cur;
    struct del_args *pos, *n;
    LIST_HEAD(local);
    int err, nosuchfod = 0, tid = ta->tid;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the enqueue events */
    while (!g_del_thread_stop[tid]) {
        atomic64_set(&g_del_thread_ticks[tid], LONG_MIN);
        err = sem_wait(&g_del_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }
        atomic64_set(&g_del_thread_ticks[tid], 0);

        cur = time(NULL);
        /* NOTE: only handle ONE request now */
        xlock_lock(&g_del_lock);
        list_for_each_entry_safe(pos, n, &g_del, list) {
            if (pos->status == DEL_STATE_INIT) {
                list_del(&pos->list);
                list_add_tail(&pos->list, &local);
                atomic64_inc(&g_di.hdel);
                break;
            }
        }
        xlock_unlock(&g_del_lock);

        /* iterate local list */
        list_for_each_entry_safe(pos, n, &local, list) {
            FILE *f;
            char cmd[8192];
            int len = 0;

            hvfs_info(lib, "[%d] Handle DEL POS target.location %s status %d\n",
                      tid, pos->target.location, pos->status);
            pos->retries++;
            switch (pos->status) {
            case DEL_STATE_INIT:
                hvfs_info(lib, "Begin Delete: TARGET{%s:%s/%s}\n",
                          pos->target.node, pos->target.mp, pos->target.location);
                pos->status = DEL_STATE_DOING;
                pos->latency = time(NULL);

                /* double check the node */
                if (strcmp(pos->target.node, g_hostname) != 0) {
                    hvfs_warning(lib, "This DEL request want to del file on node %s"
                                 ", but we are %s?\n",
                                 pos->target.node, g_hostname);
                    len += sprintf(cmd, "ssh %s ", pos->target.node);
                }
#if 1
                len += sprintf(cmd + len, "%s rm -rf %s/%s",
                               g_timeout_str,
                               pos->target.mp, pos->target.location);
#else
                len += sprintf(cmd + len, "ls %s/%s",
                               pos->target.mp, pos->target.location);
#endif
                break;
            case DEL_STATE_DOING:
                continue;
                break;
            case DEL_STATE_DONE:
                list_del(&pos->list);
                xlock_lock(&g_del_lock);
                list_add_tail(&pos->list, &g_del);
                xlock_unlock(&g_del_lock);
                atomic64_dec(&g_di.hdel);
                atomic64_inc(&g_di.ddel);
                continue;
                break;
            case DEL_STATE_ERROR:
                if (cur - pos->ttl >= g_ds_conf.fl_interval ||
                    pos->retries >= g_ds_conf.fl_max_retry) {
                    // delete it and report +FAIL
                    pos->status = DEL_STATE_ERROR_DONE;
                    pos->latency = time(NULL) - pos->latency;
                    pos->errcode = -ETIMEDOUT;
                    list_del(&pos->list);
                    xlock_lock(&g_del_lock);
                    list_add_tail(&pos->list, &g_del);
                    atomic64_dec(&g_di.hdel);
                    atomic64_inc(&g_di.ddel);
                    xlock_unlock(&g_del_lock);
                } else {
                    // otherwise reset to INIT
                    pos->status = DEL_STATE_INIT;
                }
                continue;
                break;
            }

            f = popen(cmd, "r");
            if (f == NULL) {
                hvfs_err(lib, "popen(%s) failed w/ %s\n",
                         cmd, strerror(errno));
                pos->status = DEL_STATE_ERROR;
                pos->errcode = -errno;
                continue;
            } else {
                char *line = NULL;
                size_t len = 0;
                
                while ((getline(&line, &len, f)) != -1) {
                    hvfs_info(lib, "EXEC(%s):%s", cmd, line);
                    if (strstr(line, "No such file or directory") != NULL) {
                        nosuchfod = 1;
                    } else if (strstr(line, "Input/output error") != NULL) {
                        /* IOError, which means target file's disk failed?
                         * Fail quickly */
                        pos->retries += g_ds_conf.fl_max_retry;
                        pos->errcode = -EIO;
                    }
                }
                xfree(line);
            }
            int status = pclose(f);
            
            if (WIFEXITED(status)) {
                hvfs_info(lib, "CMD(%s) exited, status=%d\n", cmd, WEXITSTATUS(status));
                if (WEXITSTATUS(status)) {
                    if (pos->status == DEL_STATE_ERROR)
                        pos->status = DEL_STATE_INIT;//reset to INIT state
                    else {
                        if (nosuchfod)
                            pos->status = DEL_STATE_DONE;
                        else {
                            pos->status = DEL_STATE_ERROR;
                            pos->errcode = -WEXITSTATUS(status);
                        }
                    }
                    /* 
                     * If get errcode 124, it means disk might be timed out?
                     */
                    if (WEXITSTATUS(status) == 124) {
                        hvfs_err(lib, "Got timedout for disk: %s(%s)\n",
                                 pos->target.devid, pos->target.mp);
                    }
                } else {
                    if (pos->status == DEL_STATE_ERROR)
                        pos->status = DEL_STATE_INIT;
                    else
		       {
  			 pos->status = DEL_STATE_DONE;
			 int ret = remove_from_hdfs(pos);
                         if(ret != 0)
                         {
                            hvfs_info(lib,"remove_from_hdfs is failed");
                            pos->errcode = EAGAIN;
                          }else{
                            hvfs_info(lib,"remove_from_hdfs is successful");
                          }
			}
                      
                }
            } else if (WIFSIGNALED(status)) {
                hvfs_info(lib, "CMD(%s) killed by signal %d\n", cmd, WTERMSIG(status));
                pos->status = DEL_STATE_ERROR;
                pos->errcode = -EINTR;
            } else if (WIFSTOPPED(status)) {
                hvfs_err(lib, "CMD(%s) stopped by signal %d\n", cmd, WSTOPSIG(status));
                pos->status = DEL_STATE_ERROR;
            } else if (WIFCONTINUED(status)) {
                hvfs_err(lib, "CMD(%s) continued\n", cmd);
                pos->status = DEL_STATE_ERROR;
            }

            /* add to g_del list if needed */
            if (pos->status == DEL_STATE_DONE) {
                if(pos->errcode == EAGAIN)
           	 {
                   int ret = remove_from_hdfs(pos);
                   if(ret != 0)
                   {
                    	hvfs_info(lib,"remove_from_hdfs is failed");
                        pos->errcode = EAGAIN;
                   }else{
                        hvfs_info(lib,"remove_from_hdfs is successful");
                        pos->errcode = 0;
                   }
            }else
            {
             pos->latency = time(NULL) - pos->latency;
             list_del(&pos->list);
             xlock_lock(&g_del_lock);
             list_add_tail(&pos->list, &g_del);
             xlock_unlock(&g_del_lock);
             atomic64_dec(&g_di.hdel);
             atomic64_inc(&g_di.ddel);
            }
            }
        }
    }

    hvfs_debug(lib, "Hooo, I am exiting ...\n");
    pthread_exit(0);
}

static int __is_dev_mounted(char *devid)
{
    int r = 0, i;

    xlock_lock(&g_dpi_lock);
    for (i = 0; i < g_nr; i++) {
        if (strcmp(devid, g_dpi[i].dev_sn) == 0) {
            r = 1;
            break;
        }
    }
    xlock_unlock(&g_dpi_lock);

    return r;
}


int upload_to_hdfs(struct rep_args *pos)
{
    FILE *f;
    FILE *f2;
    char cmd[9216];
    char hdfs_dir_cmd[9216];
    char *hdfs_root_dir = NULL;

    char *jar_path = "/home/metastore/sotstore/dservice/lib/data_trans.jar";
    hvfs_info(lib,"libing:debug:step1:go into upload_to_hdfs");
    sprintf(hdfs_dir_cmd,"java -jar %s 0",jar_path);
    hvfs_info(lib,"libing:debug:step2:hdfs_dir_cmd:%s",hdfs_dir_cmd);

    f = popen(hdfs_dir_cmd, "r");
    if(f == NULL)
        {
            hvfs_info(lib,"popen(%s) failedw/\n",hdfs_dir_cmd);

        }else{
            char *line = NULL;
            size_t len = 0;
            while((getline(&line,&len,f)) != -1)
            {
                hvfs_info(lib,"libing:debug:step3.2:EXEC(%s):%s",hdfs_dir_cmd,line);
                hdfs_root_dir = line;
                hvfs_info(lib,"libing:debug:step3.3 hdfs_root_dir:%s",hdfs_root_dir);
            }

        }
    sprintf(cmd,"java -jar %s  1 %s%s %s%s",jar_path,pos->from.mp,pos->from.location,hdfs_root_dir,pos->to.location);
    hvfs_info(lib,"libing:debug:step4:upload cmd is:%s",cmd);

        f2 = popen(cmd,"r");
        if(f2 == NULL)
        {
            hvfs_info(lib,"popen(%s) failed\n",cmd);

        }else{
            char *line = NULL;
            size_t len =0;
            while((getline(&line,&len,f2)) != -1){
                hvfs_info(lib,"libing:debug :step4.3:EXEC(%s):%s",cmd,line);

            }
        }

    int result = pclose(f2);
    if (-1 == result)
    {
      hvfs_info(lib,"pclose(%s) failedw/\n",cmd);
      exit(1);
    }
    else
    {
    if (WIFEXITED(result))
    {
      hvfs_info(lib, "CMD(%s) exited, status=%d\n", cmd, WEXITSTATUS(result));
    }
    }
    return 0;
}

static void *__rep_thread_main(void *args)
{
    struct thread_args *ta = (struct thread_args *)args;
    sigset_t set;
    time_t cur;
    struct rep_args *pos, *n;
    LIST_HEAD(local);
    int err, tid = ta->tid;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the enqueue events */
    while (!g_rep_thread_stop[tid]) {
        atomic64_set(&g_rep_thread_ticks[tid], LONG_MIN);
        err = sem_wait(&g_rep_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }
        atomic64_set(&g_rep_thread_ticks[tid], 0);

        cur = time(NULL);
        /* NOTE: only handle ONE request now */
        xlock_lock(&g_rep_lock);
        list_for_each_entry_safe(pos, n, &g_rep, list) {
            if (pos->status == REP_STATE_INIT) {
                list_del(&pos->list);
                list_add_tail(&pos->list, &local);
                atomic64_inc(&g_di.hrep);
                break;
            }
        }
        xlock_unlock(&g_rep_lock);

        /* iterate local list */
        list_for_each_entry_safe(pos, n, &local, list) {
            FILE *f;
            char cmd[8192];
            char *dir = strdup(pos->to.location);
            char *digest = NULL;

            hvfs_info(lib, "[%d] Handle REP POS to.location %s status %d\n", 
                      tid, pos->to.location, pos->status);
            pos->retries++;
            switch(pos->status) {
            case REP_STATE_INIT:
                hvfs_info(lib, "Begin Replicate: TO{%s:%s/%s} FROM{%s:%s/%s}\n",
                          pos->to.node, pos->to.mp, pos->to.location,
                          pos->from.node, pos->from.mp, pos->from.location);
                if (cur - pos->ttl >= g_ds_conf.fl_interval) {
                    /* if this request is lasting too long, reject it */
                    hvfs_warning(lib, "Reject Replicate: TO{%s:%s/%s} FROM{%s:%s/%s}\n",
                                 pos->to.node, pos->to.mp, pos->to.location,
                                 pos->from.node, pos->from.mp, pos->from.location);
                    pos->status = REP_STATE_ERROR_DONE;
                    pos->errcode = -ETIMEDOUT;
                    list_del(&pos->list);
                    xlock_lock(&g_rep_lock);
                    list_add_tail(&pos->list, &g_rep);
                    xlock_unlock(&g_rep_lock);
                    atomic64_dec(&g_di.hrep);
                    atomic64_inc(&g_di.drep);
                    continue;
                }
                /* BUG-XXX: we should check if to.mp mounted, otherwise, data
                 * might go to OS disk!!! */
                if (!__is_dev_mounted(pos->to.devid)) {
                    hvfs_warning(lib, "Unmounted Replicate: TO{%s:%s/%s} FROM{%s:%s/%s}\n",
                                 pos->to.node, pos->to.mp, pos->to.location,
                                 pos->from.node, pos->from.mp, pos->from.location);
                    pos->status = REP_STATE_ERROR_DONE;
                    pos->errcode = -EINVAL;
                    list_del(&pos->list);
                    xlock_lock(&g_rep_lock);
                    list_add_tail(&pos->list, &g_rep);
                    xlock_unlock(&g_rep_lock);
                    atomic64_dec(&g_di.hrep);
                    atomic64_inc(&g_di.drep);
                    continue;
                }
                
                pos->status = REP_STATE_DOING;
                pos->latency = time(NULL);
#if 1
                sprintf(cmd, "ssh %s 'umask -S 0 && mkdir -p %s/%s' && "
                        "ssh %s stat -t %s/%s 2>&1 && "
                        "%s %s:%s/%s/ %s%s%s/%s 2>&1 && "
                        "cd %s/%s && find . -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum",
                        pos->to.node, pos->to.mp, dirname(dir),
                        pos->from.node, pos->from.mp, pos->from.location,
                        g_copy_cmd,
                        pos->from.node, pos->from.mp, pos->from.location,
                        (g_is_rsync ? "" : pos->to.node),
                        (g_is_rsync ? "" : ":"),
                        pos->to.mp, pos->to.location,
                        pos->to.mp, pos->to.location);
#else
                sprintf(cmd, "ssh %s 'umask -S 0 && mkdir -p %s/%s' && "
                        "ssh %s stat -t %s/%s 2>&1 && "
                        "%s %s:%s/%s/ %s%s%s/%s 2>&1 && "
                        "if [ -d %s/%s ]; then cd %s/%s && "
                        "find . -type f -exec md5sum {} + | awk '{print $1}' | sort | "
                        "md5sum ; "
                        "else cd %s && "
                        "find ./%s -type f -exec md5sum {} + | awk '{print $1}' | sort | "
                        "md5sum ; fi",
                        pos->to.node, pos->to.mp, dirname(dir),
                        pos->from.node, pos->from.mp, pos->from.location,
                        g_copy_cmd,
                        pos->from.node, pos->from.mp, pos->from.location,
                        (g_is_rsync ? "" : pos->to.node),
                        (g_is_rsync ? "" : ":"),
                        pos->to.mp, pos->to.location,
                        pos->to.mp, pos->to.location,
                        pos->to.mp, pos->to.location,
                        pos->to.mp, pos->to.location);
#endif
                break;
            case REP_STATE_DOING:
                continue;
                break;
            case REP_STATE_DONE:
                list_del(&pos->list);
                xlock_lock(&g_rep_lock);
                list_add_tail(&pos->list, &g_rep);
                xlock_unlock(&g_rep_lock);
                atomic64_dec(&g_di.hrep);
                atomic64_inc(&g_di.drep);
                continue;
                break;
            case REP_STATE_ERROR:
                if (cur - pos->ttl >= g_ds_conf.fl_interval ||
                    pos->retries >= g_ds_conf.fl_max_retry) {
                    // delete it and report +FAIL
                    pos->status = REP_STATE_ERROR_DONE;
                    pos->latency = time(NULL) - pos->latency;
                    pos->errcode = -ETIMEDOUT;
                    list_del(&pos->list);
                    xlock_lock(&g_rep_lock);
                    list_add_tail(&pos->list, &g_rep);
                    xlock_unlock(&g_rep_lock);
                    atomic64_dec(&g_di.hrep);
                    atomic64_inc(&g_di.drep);
                    continue;
                } else {
                    sprintf(cmd, "%s rm -rf %s/%s",
                            g_timeout_str, pos->to.mp, pos->to.location);
                }
                break;
            }
            free(dir);
            
            f = popen(cmd, "r");
            if (f == NULL) {
                hvfs_err(lib, "popen(%s) failed w/ %s\n",
                         cmd, strerror(errno));
                /* change state to ERROR? */
                pos->status = REP_STATE_ERROR;
                pos->errcode = -errno;
                continue;
            } else {
                char *line = NULL;
                size_t len = 0;
                
                while ((getline(&line, &len, f)) != -1) {
                    hvfs_info(lib, "EXEC(%s):%s", cmd, line);
                    if (strstr(line, "  -") != NULL) {
                        char *p, *s;
                        p = strtok_r(line, " -\n", &s);
                        if (p) {
                            digest = strdup(p);
                        }
                    } else if (strstr(line, "No such file or directory") != NULL) {
                        if (g_rep_mkdir_on_nosuchfod) {
                            /* FIXME: this means we can also touch the directory
                             * (because REP means it's closed) */
                            char mkdircmd[4096];
                            FILE *lf;

                            sprintf(mkdircmd, "ssh %s mkdir -p %s/%s > /dev/null",
                                    pos->from.node,
                                    pos->from.mp, pos->from.location);

                            hvfs_warning(lib, "CMD(%s) execute ...\n", mkdircmd);
                            lf = popen(mkdircmd, "r");
                            if (lf == NULL) {
                                hvfs_err(lib, "popen(%s) failed w/ %s\n",
                                         mkdircmd, strerror(errno));
                                pos->retries += g_ds_conf.fl_max_retry;
                                pos->errcode = -errno;
                            }
                            pclose(lf);
                        } else {
                            /* fail quickly */
                            pos->retries += g_ds_conf.fl_max_retry;
                            pos->errcode = -ENOENT;
                        }
                    } else if (strstr(line, "Input/output error") != NULL) {
                        /* IOError, which means source file or target file's
                         * disk failed? Fail quickly */
                        pos->retries += g_ds_conf.fl_max_retry;
                        pos->errcode = -EIO;
                    }
                }
                xfree(line);
            }
            int status = pclose(f);
            
            if (WIFEXITED(status)) {
                hvfs_info(lib, "CMD(%s) exited, status=%d\n", cmd, WEXITSTATUS(status));
                if (WEXITSTATUS(status)) {
                    if (pos->status == REP_STATE_ERROR)
                        pos->status = REP_STATE_INIT;//reset to INIT state
                    else {
                        pos->status = REP_STATE_ERROR;
                        pos->errcode = -WEXITSTATUS(status);
                    }
                    /* 
                     * If get errcode 124, it means disk might be timed out?
                     */
                    if (WEXITSTATUS(status) == 124) {
                        hvfs_err(lib, "Got timedout for disk: %s(%s)\n",
                                 pos->to.devid, pos->to.mp);
                    }
                } else {
                    if (pos->status == REP_STATE_ERROR)
                        pos->status = REP_STATE_INIT;
                    else
                       
{
 pos->status = REP_STATE_DONE;
    hvfs_info(lib,"libing:debug:call the upload_to_hdfs function");
                         int ret = upload_to_hdfs(pos);

                         if(ret != 0)
                         {
                            hvfs_info(lib,"upload to hdfs failed!");
                            pos->errcode = EAGAIN;

                         }else{
                            hvfs_info(lib,"upload to hdfs successfully!");
                         }

}
                }
            } else if (WIFSIGNALED(status)) {
                hvfs_info(lib, "CMD(%s) killed by signal %d\n", cmd, WTERMSIG(status));
                pos->status = REP_STATE_ERROR;
                pos->errcode = -EINTR;
            } else if (WIFSTOPPED(status)) {
                hvfs_err(lib, "CMD(%s) stopped by signal %d\n", cmd, WSTOPSIG(status));
                pos->status = REP_STATE_ERROR;
            } else if (WIFCONTINUED(status)) {
                hvfs_err(lib, "CMD(%s) continued\n", cmd);
                pos->status = REP_STATE_ERROR;
            }

            /* add to g_rep list if needed */
            if (pos->status == REP_STATE_DONE) {
                 if(pos->errcode == EAGAIN)
            {
            int ret = upload_to_hdfs(pos);

                                                 if(ret != 0)
                                                 {
                                                    hvfs_info(lib,"upload to hdfs failed!");
                                                    pos->errcode = EAGAIN;

                                                 }else{
                                                    hvfs_info(lib,"upload to hdfs successfully!");
                                                    pos->errcode = 0;
                                                 }

            }else
            {
            pos->latency = time(NULL) - pos->latency;
                                            list_del(&pos->list);
                                            pos->digest = digest;
                                            xlock_lock(&g_rep_lock);
                                            list_add_tail(&pos->list, &g_rep);
                                            xlock_unlock(&g_rep_lock);
                                            atomic64_dec(&g_di.hrep);
                                            atomic64_inc(&g_di.drep);
            }
            }
        }
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

typedef void (*__dir_iterate_func)(char *path, char *name, int depth, void *data);

static inline
int __ignore_self_parent(char *dir)
{
    if ((strcmp(dir, ".") == 0) ||
        (strcmp(dir, "..") == 0)) {
        return 0;
    }
    return 1;
}

void __print_targets(char *path, char *name, int depth, void *data)
{
    char tname[PATH_MAX];

    sprintf(tname, "%s/%s", path, name);
    hvfs_info(lib, " -> %s\n", tname);
}

void __select_target(char *path, char *name, int depth, void *data)
{
    struct dev_scan_context *dsc = data;
    struct verify_args *va;
    char tname[PATH_MAX];

    if (random() % dsc->prob == 0) {
        sprintf(tname, "%s/%s", path, name);
        va = xzalloc(sizeof(*va));
        if (!va) {
            hvfs_err(lib, "xzalloc() verify_args failed, no memory\n");
            return;
        }
        INIT_LIST_HEAD(&va->list);

        va->target.devid = strdup(dsc->devid);
        va->target.location = strdup(&tname[strlen(dsc->mp)]);
        va->level = dsc->level;
    
        xlock_lock(&g_verify_lock);
        list_add_tail(&va->list, &g_verify);
        xlock_unlock(&g_verify_lock);
    }
}

static int __dir_iterate(char *parent, char *name, __dir_iterate_func func,
                         int depth, int max_depth, void *data)
{
    char path[PATH_MAX];
    struct dirent entry;
    struct dirent *result;
    DIR *d;
    int err = 0;

    if (name)
        sprintf(path, "%s/%s", parent, name);
    else
        sprintf(path, "%s", parent);
    do {
        int len = strlen(path);

        if (len == 1)
            break;
        if (path[len - 1] == '/') {
            path[len - 1] = '\0';
        } else
            break;
    } while (1);

    d = opendir(path);
    if (!d) {
        hvfs_debug(lib, "opendir(%s) failed w/ %s(%d)\n",
                   path, strerror(errno), errno);
        goto out;
    }

    for (err = readdir_r(d, &entry, &result);
         err == 0 && result != NULL;
         err = readdir_r(d, &entry, &result)) {
        /* ok, we should iterate over the dirs */
        if (entry.d_type == DT_DIR && __ignore_self_parent(entry.d_name)) {
            if (depth >= max_depth) {
                /* call the function now */
                func(path, entry.d_name, depth, data);
            } else {
                err = __dir_iterate(path, entry.d_name, func, depth + 1, 
                                    max_depth, data);
                if (err) {
                    hvfs_err(lib, "Dir %s: iterate to func failed w/ %d\n",
                             entry.d_name, err);
                }
            }
        }
    }
    closedir(d);

out:
    return err;
}

static int __device_scan(int prob, int level)
{
    struct disk_part_info *dpi = NULL;
    struct dev_scan_context dsc = {
        .level = level,
        .prob = prob,
        .nr = 0,
    };
    int nr = 0, err = 0, i;
    
    if (!g_specify_dev) {
        err = get_disk_parts(&dpi, &nr, g_devtype == NULL ? "scsi" : g_devtype);
        if (err) {
            hvfs_err(lib, "get_disk_parts() failed w/ %d\n", err);
            goto out;
        }
        
        err = fix_disk_parts(dpi, nr);
        if (err) {
            hvfs_warning(lib, "fix_disk_parts() failed w/ %s(%d), ignore "
                         "any NRs\n", strerror(-err), err);
        }
    }

    if (!dpi)
        goto out;

    /* for each device, scan mount point with 'data_path' */
    for (i = 0; i < nr; i++) {
        char data_root[NAME_MAX];

        dsc.devid = dpi[i].dev_sn;
        dsc.mp = dpi[i].mount_path;
        sprintf(data_root, "%s/%s", dpi[i].mount_path,
                g_ds_conf.data_path);
        hvfs_info(lib, "Scanning DP '%s' on device %s\n",
                  data_root, dpi[i].dev_sn);
        __dir_iterate(data_root, NULL, __select_target, 0, 2, &dsc);
    }

    dpi_free(dpi, nr);
    
out:
    return err;
}

static void *__dscan_thread_main(void *arg)
{
    static time_t last = 0;
    sigset_t set;
    time_t cur;
    int err;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the timer events */
    while (!g_dscan_thread_stop) {
        err = sem_wait(&g_dscan_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        cur = time(NULL);
        if (!last)
            last = cur;
        if (cur - last > g_ds_conf.dscan_interval) {
            err = __device_scan(g_dev_scan_prob, VERIFY_LEVEL_EXIST);
            if (err) {
                hvfs_err(lib, "__device_scan() failed w/ %s\n", strerror(errno));
            }
            last = cur;
        } else if (cur - last < 0) {
            hvfs_warning(lib, "Detect timestamp adjust backward, "
                         "reset last dscanTS.\n");
            last = cur;
        }
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

void __audit_log_handling(char *p, long len)
{
    char *q = p, *u = p;
    int i, n;
    
    for (i = 0, n = 0; i < len; i++) {
        if (*(p + i) == '\n') {
            if (g_ds_conf.enable_audit) {
                n++;
                if (n >= 20 || i == len - 1) {
                    /* Send it to server immediately */
                    struct iovec iov[2];

                    iov[0].iov_base = g_audit_header;
                    iov[0].iov_len = strlen(g_audit_header);
                    iov[1].iov_base = u;
                    iov[1].iov_len = p + i - u + 1;
                    __dgram_sendmsg(iov, 2);
                    n = 0;
                    u = p + i + 1;
                }
            } else {
                char *s = NULL, *t;
                int j;

                *(p + i) = '\0';
                hvfs_info(lib, "Got '%s'\n", q);
                for (j = 0, t = q; ++j; t = NULL) {
                    t = strtok_r(t, ",", &s);
                    if (!t) {
                        break;
                    }
                    hvfs_debug(lib, "-> Got %d %s\n", j, t);
                    switch (j) {
                    case 1:
                        /* tag */
                        break;
                    case 2:
                        /* timestamp, ms */
                        break;
                    case 3:
                        /* devid */
                        break;
                    case 4:
                        /* location */
                        break;
                    case 5:
                        /* VFSOperation */
                        break;
                    }
                }
            }
            q = p + i + 1;
        }
    }
    if (q < p + len) {
        hvfs_warning(lib, "Incomplete audit line '%s'.\n", q);
    }
}

void __audit_log_count(char *p, long len)
{
    static long tnr = 0;
    long lnr = 0, i;

    for (i = 0; i < len; i++) {
        if (*(p + i) == '\n')
            lnr++;
    }
    tnr += lnr;
    hvfs_info(lib, "Got %ld audit lines, total %ld audit lines\n", lnr, tnr);
}

void __get_audit_log()
{
    char *buf = NULL;
    int err = 0, br;
    long start, end, len;
    
    /* Step 1: open the shm */
    int fd = open_audit();
    if (fd < 0)
        goto out;

    /* Step 2: use RW lock, read in the content, and truncate it */
    lock_shm(fd, SHMLOCK_WR);
    end = lseek(fd, 0, SEEK_END);
    if (end < 0) {
        hvfs_err(lib, "lseek 1 failed w/ %s\n", strerror(errno));
        goto out_release;
    }
    start = max(0L, end - 10 * 1024 * 1024);
    len = end - start;
    err = lseek(fd, start, SEEK_SET);
    if (err < 0) {
        hvfs_err(lib, "lseek 2 failed w/ %s\n", strerror(errno));
        goto out_release;
    }
    if (len <= 0) {
        goto out_release;
    }
    
    buf = xmalloc(len + 1);
    if (!buf) {
        hvfs_err(lib, "xmalloc failed, no memory\n");
        goto out_release;
    }
    buf[len] = '\0';

    br = 0;
    do {
        err = read(fd, buf + br, len - br);
        if (err <= 0) {
            hvfs_err(lib, "read failed w/ %s\n", strerror(errno));
            goto out_free;
        }
        br += err;
    } while (br < len);

    char *p = buf;

    if (start > 0) {
        while (p < buf + len && *p != '\n') {
            p++;
        }
        p++;
    }
    
    if (end > start + (p - buf)) {
        err = ftruncate(fd, start + (p - buf));
        if (err < 0) {
            hvfs_err(lib, "ftruncate failed w/ %s\n", strerror(errno));
            goto out_free;
        }
    }

    if (p < buf + len) {
        // ok, handling the content
        __audit_log_count(p, buf + len - p);
        __audit_log_handling(p, buf + len - p);
    }

out_free:
    xfree(buf);
out_release:
    lock_shm(fd, SHMLOCK_UN);
out:
    close(fd);
}

static void *__audit_thread_main(void *arg)
{
    sigset_t set;
    time_t cur;
    static time_t last = 0;
    int err;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the timer events */
    while (!g_audit_thread_stop) {
        err = sem_wait(&g_audit_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        cur = time(NULL);
        if (last + 5 <= cur) {
            last = cur;
            __get_audit_log();
        } else if (cur - last < 0) {
            hvfs_warning(lib, "Detect timestamp adjust backward, "
                         "reset auditTS.\n");
            last = cur;
        }
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int setup_audit()
{
    int err = 0;

    err = pthread_create(&g_audit_thread, NULL, &__audit_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create audit thread failed w/ %s\n",
                 strerror(errno));
        err = -errno;
        goto out;
    }
out:
    return err;
}

static void *__liveness_thread_main(void *arg)
{
    sigset_t set;
    int err;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the timer events */
    while (!g_liveness_thread_stop) {
        err = sem_wait(&g_liveness_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        check_thread_liveness();
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int setup_liveness()
{
    int err = 0;

    err = pthread_create(&g_liveness_thread, NULL, &__liveness_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create liveness thread failed w/ %s\n",
                 strerror(errno));
        err = -errno;
        goto out;
    }
out:
    return err;
}

int main(int argc, char *argv[])
{
    struct disk_info *di = NULL;
    struct disk_part_info *dpi = NULL;
    int nr = 0, nr2 = 0;
    int err = 0, m_cc_set = 0;

#ifndef COMPILE_DATE
#define COMPILE_DATE "Unknown Date"
#endif
#ifndef COMPILE_HOST
#define COMPILE_HOST "localhost"
#endif
#ifndef GIT_SHA
#define GIT_SHA "master"
#endif

    memset(&g_di, 0, sizeof(g_di));
    g_di.recvts = g_di.upts = time(NULL);
    srandom(g_di.upts);
    hvfs_plain(lib, "Build Info: %s compiled at %s on %s\ngit-sha %s\n", argv[0], 
               COMPILE_DATE, COMPILE_HOST, GIT_SHA);

    char *shortflags = "r:p:t:d:h?f:m:xT:o:O:I:b:M:SR:D:C:AK:k:";
    struct option longflags[] = {
        {"server", required_argument, 0, 'r'},
        {"port", required_argument, 0, 'p'},
        {"host", required_argument, 0, 't'},
        {"dev", required_argument, 0, 'd'},
        {"sdfilter", required_argument, 0, 'f'},
        {"mpfilter", required_argument, 0, 'm'},
        {"mkdirs", no_argument, 0, 'x'},
        {"devtype", required_argument, 0, 'T'},
        {"timeo", required_argument, 0, 'o'},
        {"thr_timeo", required_argument, 0, 'O'},
        {"iph", required_argument, 0, 'I'},
        {"prob", required_argument, 0, 'b'},
        {"dscan", no_argument, 0, 'S'},
        {"mkd", required_argument, 0, 'M'},
        {"reptn", required_argument, 0, 'R'},
        {"deltn", required_argument, 0, 'D'},
        {"cpcmd", required_argument, 0, 'C'},
        {"audit", no_argument, 0, 'A'},
        {"tickR", required_argument, 0, 'K'},
        {"tickD", required_argument, 0, 'k'},
        {"help", no_argument, 0, 'h'},
    };

    while (1) {
        int longindex = -1;
        int opt = getopt_long(argc, argv, shortflags, longflags, &longindex);
        if (opt == -1)
            break;
        switch (opt) {
        case '?':
        case 'h':
            do_help();
            return 0;
            break;
        case 'r':
            g_server_str = strdup(optarg);
            break;
        case 'p':
            g_port = atoi(optarg);
            break;
        case 't':
            g_hostname = strdup(optarg);
            break;
        case 'd':
            /* dev_str is dev:mount_point:xx,xx,xx */
            g_specify_dev = 1;
            g_dev_str = strdup(optarg);
            break;
        case 'x':
            g_rep_mkdir_on_nosuchfod = 1;
            break;
        case 'T':
            g_devtype = strdup(optarg);
            break;
        case 'o':
            g_ds_conf.hb_interval = atoi(optarg);
            break;
        case 'O':
            g_timeout_str = strdup(optarg);
            break;
        case 'I':
            g_ds_conf.addr_filter = strdup(optarg);
            break;
        case 'b':
            g_dev_scan_prob = atoi(optarg);
            break;
        case 'M':
            g_ds_conf.max_keeping_days = atoi(optarg);
            break;
        case 'S':
            g_ds_conf.enable_dscan = 1;
            break;
        case 'A':
            g_ds_conf.enable_audit = 1;
            break;
        case 'R':
            g_rep_thread_nr = atoi(optarg);
            if (!g_rep_thread_nr)
                g_rep_thread_nr = 1;
            break;
        case 'D':
            g_del_thread_nr = atoi(optarg);
            if (!g_del_thread_nr)
                g_del_thread_nr = 1;
            break;
        case 'C':
            g_copy_cmd = strdup(optarg);
            if (strncmp(g_copy_cmd, "rsync", 5) == 0) {
                g_is_rsync = 1;
            }
            m_cc_set = 1;
            break;
        case 'f':
        {
            // parse the filter string, add them to g_ds_conf.sdfilter
            char *fstr = strdup(optarg);
            char *p = fstr;
            int i;

            for (i = 0; i < MAX_FILTERS; i++, p = NULL) {
                p = strtok(p, ",;");
                if (!p) {
                    break;
                }
                g_ds_conf.sdfilter[i] = strdup(p);
                g_ds_conf.sdfilter_len = i + 1;
                hvfs_info(lib, "GOT NEW SD filter %d: %s\n", i, p);
            }
            xfree(fstr);
            break;
        }
        case 'm':
        {
            // parse the filter string, add them to g_ds_conf.mpfilter
            char *fstr = strdup(optarg);
            char *p = fstr;
            int i;

            for (i = 0; i < MAX_FILTERS; i++, p = NULL) {
                p = strtok(p, ",;");
                if (!p) {
                    break;
                }
                g_ds_conf.mpfilter[i] = strdup(p);
                g_ds_conf.mpfilter_len = i + 1;
                hvfs_info(lib, "GOT NEW MP filter %d: %s\n", i, p);
            }
            xfree(fstr);
            break;
        }
        case 'K':
        {
            g_ds_conf.rep_max_ticks = atol(optarg);
            if (g_ds_conf.rep_max_ticks <= 0)
                g_ds_conf.rep_max_ticks = DS_REP_MAX_TICKS;
            break;
        }
        case 'k':
        {
            g_ds_conf.del_max_ticks = atol(optarg);
            if (g_ds_conf.del_max_ticks <= 0)
                g_ds_conf.del_max_ticks = DS_DEL_MAX_TICKS;
            break;
        }
        default:
            hvfs_err(lib, "Invalid arguments!\n");
            return EINVAL;
        }
    }

    if (!g_hostname) {
        g_hostname = xzalloc(256);
        if (!g_hostname) {
            hvfs_err(lib, "xzalloc() hostname buffer failed\n");
            return ENOMEM;
        }
        err = gethostname(g_hostname, 255);
        if (err) {
            hvfs_err(lib, "gethostname failed w/ %s\n", strerror(errno));
            return errno;
        }
        hvfs_info(lib, "Get hostname as '%s'\n", g_hostname);
    }

    /* refix g_copy_cmd */
    if (g_timeout_str != NULL) {
        char __cmd[1024];

        snprintf(__cmd, 1023, "%s %s", g_timeout_str, g_copy_cmd);
        if (m_cc_set)
            xfree(g_copy_cmd);
        g_copy_cmd = strdup(__cmd);
        hvfs_info(lib, "Reset g_copy_cmd to '%s'\n", g_copy_cmd);
    }

#if 0
    {
        char ip[NI_MAXHOST];
        char query[65536];
        struct verify_args *pos3, *n3;
        int len = 0;

        memset(ip, 0, sizeof(ip));
        __convert_host_to_ip(g_hostname, ip);
        hvfs_info(lib, "TEST CONVERTION: host '%s' to ip '%s'\n", 
                  g_hostname, ip);
        __device_scan(g_dev_scan_prob, VERIFY_LEVEL_EXIST);

        xlock_lock(&g_verify_lock);
        list_for_each_entry_safe(pos3, n3, &g_verify, list) {
            hvfs_info(lib, "POS VERIFY dev %s loc %s lvl %d\n",
                      pos3->target.devid, pos3->target.location, pos3->level);
            len += sprintf(query + len, "+VERIFY:%s,%s,%s,%d\n",
                           g_hostname, pos3->target.devid,
                           pos3->target.location, pos3->level);
            list_del(&pos3->list);
            __free_verify_args(pos3);
            xfree(pos3);
        }
        xlock_unlock(&g_verify_lock);
        
        return 0;
    }
#endif

    sem_init(&g_main_sem, 0, 0);
    sem_init(&g_rep_sem, 0, 0);
    sem_init(&g_del_sem, 0, 0);
    sem_init(&g_async_recv_sem, 0, 0);
    sem_init(&g_dscan_sem, 0, 0);
    sem_init(&g_audit_sem, 0, 0);

#if 0
    char *str;
    char *line;
    int off = 0;

    str = malloc(100);
    sprintf(str, "+OK\n+REP:{sdfdsfff}\n+REP{asfsdf}");
    while ((line = getaline(str, &off)) != NULL) {
        hvfs_info(lib, "LINE: %s\n", line);
    }
    exit(0);
#endif

    xlock_init(&g_rep_lock);
    xlock_init(&g_del_lock);
    xlock_init(&g_dpi_lock);
    xlock_init(&g_dtrace_lock);

    /* setup signals */
    err = __init_signal();
    if (err) {
        hvfs_err(lib, "Init signals failed w/ %d\n", err);
        goto out_signal;
    }

    /* setup dgram */
    if (!g_server_str)
        g_server_str = "localhost";
    err = setup_dgram(g_server_str, g_port);
    if (err) {
        hvfs_err(lib, "Init datagram failed w/ %d\n", err);
        goto out_dgram;
    }

    /* setup timers */
    err = setup_timers(0);
    if (err) {
        hvfs_err(lib, "Init timers failed w/ %d\n", err);
        goto out_timers;
    }

    /* setup rep thread */
    {
        struct thread_args *ta;
        int i;

        ta = xzalloc(g_rep_thread_nr * sizeof(*ta));
        if (!ta) {
            hvfs_err(lib, "xzalloc(%d) thread_args failed, no memory.\n",
                     g_rep_thread_nr);
            err = -ENOMEM;
            goto out_rep;
        }
        for (i = 0; i < g_rep_thread_nr; i++) {
            ta[i].tid = i;
        }
        g_rep_thread_stop = xzalloc(g_rep_thread_nr * sizeof(int));
        if (!g_rep_thread_stop) {
            hvfs_err(lib, "xzalloc(%d) int failed, no memory.\n",
                     g_rep_thread_nr);
            err = -ENOMEM;
            goto out_rep;
        }
        g_rep_thread_ticks = xzalloc(g_rep_thread_nr * sizeof(atomic64_t));
        if (!g_rep_thread_ticks) {
            hvfs_err(lib, "xzalloc(%d) long failed, no memory.\n",
                     g_rep_thread_nr);
            err = -ENOMEM;
            goto out_rep;
        }
        for (i = 0; i < g_rep_thread_nr; i++) {
            atomic64_set(&g_rep_thread_ticks[i], LONG_MIN);
        }
        g_rep_thread = xzalloc(g_rep_thread_nr * sizeof(pthread_t));
        if (!g_rep_thread) {
            hvfs_err(lib, "xzalloc(%d) pthread_t failed, no memory.\n",
                     g_rep_thread_nr);
            err = -ENOMEM;
            goto out_rep;
        }

        for (i = 0; i < g_rep_thread_nr; i++) {
            err = pthread_create(&g_rep_thread[i], NULL, &__rep_thread_main,
                                 &ta[i]);
            if (err) {
                hvfs_err(lib, "Create REP thread failed w/ %s\n", strerror(errno));
                err = -errno;
                goto out_rep;
            }
        }
    }
    
    /* setup del thread */
    {
        struct thread_args *ta;
        int i;

        ta = xzalloc(g_del_thread_nr * sizeof(*ta));
        if (!ta) {
            hvfs_err(lib, "xzalloc(%d) thread_args failed, no memory.\n",
                     g_del_thread_nr);
            err = -ENOMEM;
            goto out_del;
        }
        for (i = 0; i < g_del_thread_nr; i++) {
            ta[i].tid = i;
        }
        g_del_thread_stop = xzalloc(g_del_thread_nr * sizeof(int));
        if (!g_del_thread_stop) {
            hvfs_err(lib, "xzalloc(%d) int failed, no memory.\n",
                     g_del_thread_nr);
            err = -ENOMEM;
            goto out_del;
        }
        g_del_thread_ticks = xzalloc(g_del_thread_nr * sizeof(atomic64_t));
        if (!g_del_thread_ticks) {
            hvfs_err(lib, "xzalloc(%d) long failed, no memory.\n",
                     g_del_thread_nr);
            err = -ENOMEM;
            goto out_del;
        }
        for (i = 0; i < g_del_thread_nr; i++) {
            atomic64_set(&g_del_thread_ticks[i], LONG_MIN);
        }

        g_del_thread = xzalloc(g_del_thread_nr * sizeof(pthread_t));
        if (!g_del_thread) {
            hvfs_err(lib, "xzalloc(%d) pthread_t failed, no memory.\n",
                     g_del_thread_nr);
            err = -ENOMEM;
            goto out_del;
        }

        for (i = 0; i < g_del_thread_nr; i++) {
            err = pthread_create(&g_del_thread[i], NULL, &__del_thread_main,
                                 &ta[i]);
            if (err) {
                hvfs_err(lib, "Create DEL thread failed w/ %s\n", strerror(errno));
                err = -errno;
                goto out_del;
            }
        }
    }
    
    /* setup async recv thread */
    err = pthread_create(&g_async_recv_thread, NULL, &__async_recv_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create ASYNC RECV thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out_async_recv;
    }
    sem_post(&g_async_recv_sem);

    /* setup dscan thread */
    if (g_ds_conf.enable_dscan) {
        err = pthread_create(&g_dscan_thread, NULL, &__dscan_thread_main,
                             NULL);
        if (err) {
            hvfs_err(lib, "Create DSCAN thread failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out_dscan;
        }
    }

    /* setup audit thread */
    sprintf(g_audit_header, "+node:%s\n+CMD\n+CMD\n", g_hostname);
    err = setup_audit();
    if (err) {
        hvfs_err(lib, "setup_audit() failed w/ %d\n", err);
        goto out_audit;
    }

    /* setup liveness check thread */
    err = setup_liveness();
    if (err) {
        hvfs_err(lib, "setup_liveness() failed w/ %d\n", err);
        goto out_liveness;
    }

    get_disks(&di, &nr, g_devtype == NULL ? "scsi" : g_devtype);
    di_free(di, nr);
    
    get_disk_parts(&dpi, &nr2, g_devtype == NULL ? "scsi" : g_devtype);

    hvfs_info(lib, "Got NR %d\n", nr);
    hvfs_info(lib, "Got NR %d\n", nr2);

    int fd = open_shm(O_TRUNC);
    write_shm(fd, dpi, nr2);
    //fix_disk_parts(dpi, nr2);
    dpi_free(dpi, nr2);
//    unlink_shm();

    /* loop forever */
    while (1) {
        sem_wait(&g_main_sem);
        if (g_main_thread_stop)
            break;
    }

    /* exit other threads */
    if (g_ds_conf.enable_dscan) {
        g_dscan_thread_stop = 1;
        if (g_dscan_thread) {
            sem_post(&g_dscan_sem);
            pthread_join(g_dscan_thread, NULL);
        }
    }
    g_async_recv_thread_stop = 1;
    if (g_async_recv_thread) {
        sem_post(&g_async_recv_sem);
        pthread_join(g_async_recv_thread, NULL);
    }
    g_timer_thread_stop = 1;
    if (g_timer_thread) {
        sem_post(&g_timer_sem);
        pthread_join(g_timer_thread, NULL);
    }
    g_audit_thread_stop = 1;
    if (g_audit_thread) {
        sem_post(&g_audit_sem);
        pthread_join(g_audit_thread, NULL);
    }
    g_liveness_thread_stop = 1;
    if (g_liveness_thread) {
        sem_post(&g_liveness_sem);
        pthread_join(g_liveness_thread, NULL);
    }
    {
        int i;

        for (i = 0; i < g_rep_thread_nr; i++) {
            g_rep_thread_stop[i] = 1;
            if (g_rep_thread[i]) {
                sem_post(&g_rep_sem);
            }
        }
        for (i = 0; i < g_rep_thread_nr; i++) {
            if (g_rep_thread[i]) {
                pthread_join(g_rep_thread[i], NULL);
            }
        }
    }
    {
        int i;

        for (i = 0; i < g_del_thread_nr; i++) {
            g_del_thread_stop[i] = 1;
            if (g_del_thread[i]) {
                sem_post(&g_del_sem);
            }
        }
        for (i = 0; i < g_del_thread_nr; i++) {
            if (g_del_thread[i]) {
                pthread_join(g_del_thread[i], NULL);
            }
        }
    }

    close(g_sockfd);

    hvfs_info(lib, "Main thread exiting ...\n");

out_liveness:
out_audit:
out_dscan:
out_async_recv:
out_del:
out_rep:
out_timers:
out_dgram:
out_signal:
    return err;
}
