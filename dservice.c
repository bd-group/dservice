/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2014-01-08 13:52:16 macan>
 *
 */

#include "common.h"
#include "jsmn.h"

#define MAX_FILTERS     10
struct dservice_conf
{
    char *sdfilter[MAX_FILTERS];
    char *mpfilter[MAX_FILTERS];
    int sdfilter_len;
    int mpfilter_len;
    int hb_interval;
    int mr_interval;
    int fl_interval; // fail interval
    int fl_max_retry;
    char *addr_filter;
};

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
    .hb_interval = 5,
    .mr_interval = 10,
    .fl_interval = 3600,
    .fl_max_retry = 100,
    .addr_filter = NULL,
};

static sem_t g_timer_sem;
static sem_t g_main_sem;
static sem_t g_rep_sem;
static sem_t g_del_sem;
static sem_t g_async_recv_sem;

static pthread_t g_timer_thread = 0;
static pthread_t g_rep_thread = 0;
static pthread_t g_del_thread = 0;
static pthread_t g_async_recv_thread = 0;

static int g_timer_thread_stop = 0;
static int g_rep_thread_stop = 0;
static int g_del_thread_stop = 0;
static int g_main_thread_stop = 0;
static int g_async_recv_thread_stop = 0;

static char *g_devtype = NULL;

static int g_sockfd;
static struct sockaddr_in g_server;
static char *g_server_str = NULL;
static int g_port = 20202;

static int g_specify_dev = 0;
static char *g_dev_str = NULL;
static char *g_hostname = NULL;

static xlock_t g_rep_lock;
static LIST_HEAD(g_rep);
static xlock_t g_del_lock;
static LIST_HEAD(g_del);

#define NORMAL_HB_MODE          0
#define QUICK_HB_MODE           1

static int g_rep_mkdir_on_nosuchfod = 0;

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
    } else if (signo == SIGHUP || signo == SIGINT) {
        if (!g_main_thread_stop) {
            hvfs_info(lib, "Exit DService ...\n");
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
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    struct sockaddr_in *si;
    int err = 0, copied = 0;

    if (!g_ds_conf.addr_filter) {
        strcpy(ip, host);
        return;
    }
    
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    err = getaddrinfo(host, "0", &hints, &result);
    if (err) {
        hvfs_warning(lib, "getaddrinfo() failed, use hostname.\n");
        strcpy(ip, host);
        return;
    }
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        si = (struct sockaddr_in *)rp->ai_addr;
        char p[64];
        if (inet_ntop(AF_INET, &si->sin_addr, p, sizeof(p)) != NULL) {
            if (strstr(p, g_ds_conf.addr_filter) != NULL) {
                /* ok, use this IP */
                strcpy(ip, p);
                copied = 1;
                break;
            }
        }
    }
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
            t[i].read_nr = 0;
            t[i].write_nr = 0;
            t[i].err_nr = 0;

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
            t[i].read_nr = 0;
            t[i].write_nr = 0;
            t[i].err_nr = 0;

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
        /* find and update */
        for (i = 0; i < nr; i++) {
            if(strcmp(dpi[i].dev_sn, this.dev_sn) == 0) {
                dpi[i].read_nr = this.read_nr;
                dpi[i].write_nr = this.write_nr;
                dpi[i].err_nr = this.err_nr;
                break;
            }
        }
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
    int err = 0;
    
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
    socklen_t serverSize = sizeof(g_server);
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
                       (struct sockaddr*) &g_server, &serverSize)) == -1) {
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
int __dgram_send(char *send)
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

void refresh_map(time_t cur)
{
    static time_t last = 0;
    struct disk_part_info *dpi = NULL;
    int nr = 0, fd, err;

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
        dpi_free(dpi, nr);

        hvfs_info(lib, "Map refreshed!\n");
    update_last:
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

            xlock_lock(&g_rep_lock);
            list_add_tail(&ra->list, &g_rep);
            xlock_unlock(&g_rep_lock);
            sem_post(&g_rep_sem);

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

            has_cmds = 1;
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

int __do_heartbeat()
{
    char query[64 * 1024];
    struct rep_args *pos, *n;
    struct del_args *pos2, *n2;
    int err = 0, nr2 = 0, nr_max = 10;

    struct disk_part_info *dpi = NULL;
    int nr = 0, i, len = 0, mode = NORMAL_HB_MODE;
    
    memset(query, 0, sizeof(query));
    len += sprintf(query, "+node:%s\n", g_hostname);
    
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
        
        for (i = 0; i < nr; i++) {
            len += sprintf(query + len, "%s:%s,%ld,%ld,%ld,%ld,%ld\n",
                           dpi[i].dev_sn, dpi[i].mount_path,
                           dpi[i].read_nr, dpi[i].write_nr, dpi[i].err_nr,
                           dpi[i].used, dpi[i].free);
        }
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
    xlock_lock(&g_rep_lock);
    list_for_each_entry_safe(pos, n, &g_rep, list) {
        if (nr2 >= nr_max) {
            mode = QUICK_HB_MODE;
            break;
        }
        hvfs_info(lib, "POS REP_R to.location %s status %d\n",
                  pos->to.location, pos->status);
        if (pos->status == REP_STATE_DONE) {
            len += sprintf(query + len, "+REP:%s,%s,%s,%s\n",
                           pos->to.node, pos->to.devid, pos->to.location,
                           pos->digest);
            list_del(&pos->list);
            __free_rep_args(pos);
            xfree(pos);
            nr2++;
        } else if (pos->status == REP_STATE_ERROR_DONE) {
            len += sprintf(query + len, "+FAIL:REP:%s,%s,%s\n",
                           pos->to.node, pos->to.devid, pos->to.location);
            list_del(&pos->list);
            __free_rep_args(pos);
            xfree(pos);
            nr2++;
        }
    }
    xlock_unlock(&g_rep_lock);
    
    xlock_lock(&g_del_lock);
    list_for_each_entry_safe(pos2, n2, &g_del, list) {
        if (nr2 >= nr_max) {
            mode = QUICK_HB_MODE;
            break;
        }
        hvfs_info(lib, "POS DEL_R target.location %s status %d\n",
                  pos2->target.location, pos2->status);
        if (pos2->status == DEL_STATE_DONE) {
            len += sprintf(query + len, "+DEL:%s,%s,%s\n",
                           pos2->target.node, pos2->target.devid, 
                           pos2->target.location);
            list_del(&pos2->list);
            __free_del_args(pos2);
            xfree(pos2);
            nr2++;
        } else if (pos2->status == DEL_STATE_ERROR_DONE) {
            len += sprintf(query + len, "+FAIL:DEL:%s,%s,%s\n",
                           pos2->target.node, pos2->target.devid, 
                           pos2->target.location);
            list_del(&pos2->list);
            __free_del_args(pos2);
            xfree(pos2);
            nr2++;
        }
    }
    xlock_unlock(&g_del_lock);
    
    err = __dgram_send(query);
    if (err) {
        hvfs_err(lib, "datagram send/recv failed w/ %s\n", 
                 strerror(-err));
    }
    
#else
    hvfs_plain(lib, "%s", query);
#endif
    dpi_free(dpi, nr);

out:;
    return mode;
}

void do_heartbeat(time_t cur)
{
    static time_t last = 0;
    static int hb_interval = 0;

    if (cur - last >= hb_interval) {
        if (__do_heartbeat() == QUICK_HB_MODE)
            hb_interval = 1;
        else
            hb_interval = g_ds_conf.hb_interval;
        last = cur;
    }
}

static void __itimer_default(int signo, siginfo_t *info, void *arg)
{
    sem_post(&g_timer_sem);
    hvfs_verbose(lib, "Did this signal handler called?\n");

    return;
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
        /* TODO: */
        do_heartbeat(cur);
        refresh_map(cur);
        /* trigger incomplete requests if they exists */
        sem_post(&g_rep_sem);
        sem_post(&g_del_sem);
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
               "Version 1.0.0b\n"
               "Copyright (c) 2013 IIE and Ma Can <ml.macana@gmail.com>\n\n"
               "Arguments:\n"
               "-r, --server      DiskManager server IP address.\n"
               "-p, --port        UDP port of DiskManager.\n"
               "-t, --host        Set local host name.\n"
               "-d, --dev         Use this specified dev: DEVID:MOUNTPOINT.\n"
               "-x, --mkdirs      Try to create the non-exist directory to REP success.\n"
               "-T, --devtype     Specify the disk type: e.g. scsi, ata, usb, ...\n"
               "-h, -?, -help     print this help.\n"
        );
}

static void *__async_recv_thread_main(void *args)
{
    sigset_t set;
    socklen_t serverSize = sizeof(g_server);
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
            if ((br == recvfrom(g_sockfd, reply, sizeof(reply), 0,
                                (struct sockaddr*) &g_server, &serverSize)) == -1) {
                if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                    hvfs_err(lib, "Recv TIMEOUT!\n");
                } else {
                    hvfs_err(lib, "Recv from Server reply failed w/ %s\n", strerror(errno));
                }
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
    pthread_exit(0);
}

static void *__del_thread_main(void *args)
{
    sigset_t set;
    time_t cur;
    struct del_args *pos, *n;
    LIST_HEAD(local);
    int err, nosuchfod = 0;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the enqueue events */
    while (!g_del_thread_stop) {
        err = sem_wait(&g_del_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        cur = time(NULL);
        /* TODO: */
        xlock_lock(&g_del_lock);
        list_for_each_entry_safe(pos, n, &g_del, list) {
            if (pos->status == DEL_STATE_INIT) {
                list_del(&pos->list);
                list_add_tail(&pos->list, &local);
            }
        }
        xlock_unlock(&g_del_lock);

        /* iterate local list */
        list_for_each_entry_safe(pos, n, &local, list) {
            FILE *f;
            char cmd[8192];
            int len = 0;

            hvfs_info(lib, "Handle DEL POS target.location %s status %d\n",
                      pos->target.location, pos->status);
            pos->retries++;
            switch (pos->status) {
            case DEL_STATE_INIT:
                hvfs_info(lib, "Begin Delete: TARGET{%s:%s/%s}\n",
                          pos->target.node, pos->target.mp, pos->target.location);
                pos->status = DEL_STATE_DOING;
                /* double check the node */
                if (strcmp(pos->target.node, g_hostname) != 0) {
                    hvfs_warning(lib, "This DEL request want to del file on node %s"
                                 ", but we are %s?\n",
                                 pos->target.node, g_hostname);
                    len += sprintf(cmd, "ssh %s ", pos->target.node);
                }
#if 1
                len += sprintf(cmd + len, "rm -rf %s/%s",
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
                continue;
                break;
            case DEL_STATE_ERROR:
                if (cur - pos->ttl >= g_ds_conf.fl_interval ||
                    pos->retries >= g_ds_conf.fl_max_retry) {
                    // delete it and report +FAIL
                    pos->status = DEL_STATE_ERROR_DONE;
                    list_del(&pos->list);
                    xlock_lock(&g_del_lock);
                    list_add_tail(&pos->list, &g_del);
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
                continue;
            } else {
                char *line = NULL;
                size_t len = 0;
                
                while ((getline(&line, &len, f)) != -1) {
                    hvfs_info(lib, "EXEC(%s):%s", cmd, line);
                    if (strstr(line, "No such file or directory") != NULL) {
                        nosuchfod = 1;
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
                        else
                            pos->status = DEL_STATE_ERROR;
                    }
                } else {
                    if (pos->status == DEL_STATE_ERROR)
                        pos->status = DEL_STATE_INIT;
                    else
                        pos->status = DEL_STATE_DONE;
                }
            } else if (WIFSIGNALED(status)) {
                hvfs_info(lib, "CMD(%s) killed by signal %d\n", cmd, WTERMSIG(status));
                pos->status = DEL_STATE_ERROR;
            } else if (WIFSTOPPED(status)) {
                hvfs_err(lib, "CMD(%s) stopped by signal %d\n", cmd, WSTOPSIG(status));
                pos->status = DEL_STATE_ERROR;
            } else if (WIFCONTINUED(status)) {
                hvfs_err(lib, "CMD(%s) continued\n", cmd);
                pos->status = DEL_STATE_ERROR;
            }

            /* add to g_del list if needed */
            if (pos->status == DEL_STATE_DONE) {
                list_del(&pos->list);
                xlock_lock(&g_del_lock);
                list_add_tail(&pos->list, &g_del);
                xlock_unlock(&g_del_lock);
            }
        }
    }

    hvfs_debug(lib, "Hooo, I am exiting ...\n");
    pthread_exit(0);
}

static void *__rep_thread_main(void *args)
{
    sigset_t set;
    time_t cur;
    struct rep_args *pos, *n;
    LIST_HEAD(local);
    int err;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the enqueue events */
    while (!g_rep_thread_stop) {
        err = sem_wait(&g_rep_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        cur = time(NULL);
        /* TODO: */
        xlock_lock(&g_rep_lock);
        list_for_each_entry_safe(pos, n, &g_rep, list) {
            if (pos->status == REP_STATE_INIT) {
                list_del(&pos->list);
                list_add_tail(&pos->list, &local);
            }
        }
        xlock_unlock(&g_rep_lock);

        /* iterate local list */
        list_for_each_entry_safe(pos, n, &local, list) {
            FILE *f;
            char cmd[8192];
            char *dir = strdup(pos->to.location);
            char *digest = NULL;

            hvfs_info(lib, "Handle REP POS to.location %s status %d\n", 
                      pos->to.location, pos->status);
            pos->retries++;
            switch(pos->status) {
            case REP_STATE_INIT:
                hvfs_info(lib, "Begin Replicate: TO{%s:%s/%s} FROM{%s:%s/%s}\n",
                          pos->to.node, pos->to.mp, pos->to.location,
                          pos->from.node, pos->from.mp, pos->from.location);
                pos->status = REP_STATE_DOING;
#if 1
                sprintf(cmd, "ssh %s umask -S 0 && mkdir -p %s/%s && "
                        "ssh %s stat -t %s/%s 2>&1 && "
                        "scp -qpr %s:%s/%s/ %s:%s/%s 2>&1 && "
                        "cd %s/%s && find . -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum",
                        pos->to.node, pos->to.mp, dirname(dir),
                        pos->from.node, pos->from.mp, pos->from.location,
                        pos->from.node, pos->from.mp, pos->from.location,
                        pos->to.node, pos->to.mp, pos->to.location,
                        pos->to.mp, pos->to.location);
#else
                sprintf(cmd, "ssh %s umask -S 0 && mkdir -p %s/%s && "
                        "ssh %s stat -t %s/%s 2>&1 && "
                        "scp -qpr %s:%s/%s/ %s:%s/%s 2>&1 && "
                        "if [ -d %s/%s ]; then cd %s/%s && "
                        "find . -type f -exec md5sum {} + | awk '{print $1}' | sort | "
                        "md5sum ; "
                        "else cd %s && "
                        "find ./%s -type f -exec md5sum {} + | awk '{print $1}' | sort | "
                        "md5sum ; fi",
                        pos->to.node, pos->to.mp, dirname(dir),
                        pos->from.node, pos->from.mp, pos->from.location,
                        pos->from.node, pos->from.mp, pos->from.location,
                        pos->to.node, pos->to.mp, pos->to.location,
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
                continue;
                break;
            case REP_STATE_ERROR:
                if (cur - pos->ttl >= g_ds_conf.fl_interval ||
                    pos->retries >= g_ds_conf.fl_max_retry) {
                    // delete it and report +FAIL
                    pos->status = REP_STATE_ERROR_DONE;
                    list_del(&pos->list);
                    xlock_lock(&g_rep_lock);
                    list_add_tail(&pos->list, &g_rep);
                    xlock_unlock(&g_rep_lock);
                    continue;
                } else {
                    sprintf(cmd, "rm -rf %s/%s", pos->to.mp, pos->to.location);
                }
                break;
            }
            free(dir);
            
            f = popen(cmd, "r");
            if (f == NULL) {
                hvfs_err(lib, "popen(%s) failed w/ %s\n",
                         cmd, strerror(errno));
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
                            }
                            pclose(lf);
                        } else {
                            /* fail quickly */
                            pos->retries += g_ds_conf.fl_max_retry;
                        }
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
                    else
                        pos->status = REP_STATE_ERROR;
                } else {
                    if (pos->status == REP_STATE_ERROR)
                        pos->status = REP_STATE_INIT;
                    else
                        pos->status = REP_STATE_DONE;
                }
            } else if (WIFSIGNALED(status)) {
                hvfs_info(lib, "CMD(%s) killed by signal %d\n", cmd, WTERMSIG(status));
                pos->status = REP_STATE_ERROR;
            } else if (WIFSTOPPED(status)) {
                hvfs_err(lib, "CMD(%s) stopped by signal %d\n", cmd, WSTOPSIG(status));
                pos->status = REP_STATE_ERROR;
            } else if (WIFCONTINUED(status)) {
                hvfs_err(lib, "CMD(%s) continued\n", cmd);
                pos->status = REP_STATE_ERROR;
            }

            /* add to g_rep list if needed */
            if (pos->status == REP_STATE_DONE) {
                list_del(&pos->list);
                pos->digest = digest;
                xlock_lock(&g_rep_lock);
                list_add_tail(&pos->list, &g_rep);
                xlock_unlock(&g_rep_lock);
            }
        }
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int main(int argc, char *argv[])
{
    struct disk_info *di = NULL;
    struct disk_part_info *dpi = NULL;
    int nr = 0, nr2 = 0;
    int err = 0;

#ifndef COMPILE_DATE
#define COMPILE_DATE "Unknown Date"
#endif
#ifndef COMPILE_HOST
#define COMPILE_HOST "localhost"
#endif
#ifndef GIT_SHA
#define GIT_SHA "master"
#endif

    hvfs_plain(lib, "Build Info: %s compiled at %s on %s\ngit-sha %s\n", argv[0], 
               COMPILE_DATE, COMPILE_HOST, GIT_SHA);

    char *shortflags = "r:p:t:d:h?f:m:xT:o:";
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
        {"iph", required_argument, 0, 'I'},
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
        case 'I':
            g_ds_conf.addr_filter = strdup(optarg);
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

    sem_init(&g_main_sem, 0, 0);
    sem_init(&g_rep_sem, 0, 0);
    sem_init(&g_del_sem, 0, 0);
    sem_init(&g_async_recv_sem, 0, 0);

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
    err = pthread_create(&g_rep_thread, NULL, &__rep_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create REP thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out_rep;
    }

    /* setup del thread */
    err = pthread_create(&g_del_thread, NULL, &__del_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create DEL thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out_del;
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
    g_rep_thread_stop = 1;
    if (g_rep_thread) {
        sem_post(&g_rep_sem);
        pthread_join(g_rep_thread, NULL);
    }
    g_del_thread_stop = 1;
    if (g_del_thread) {
        sem_post(&g_del_sem);
        pthread_join(g_del_thread, NULL);
    }

    close(g_sockfd);

    hvfs_info(lib, "Main thread exiting ...\n");

out_async_recv:
out_del:
out_rep:
out_timers:
out_dgram:
out_signal:
    return err;
}
