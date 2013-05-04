/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-05-04 12:01:04 macan>
 *
 */

#include "common.h"

struct dservice_conf
{
    int hb_interval;
    int mr_interval;
};

static struct dservice_conf g_ds_conf = {
    .hb_interval = 5,
    .mr_interval = 10,
};

static sem_t g_timer_sem;
static sem_t g_main_sem;
static pthread_t g_timer_thread = 0;
static int g_timer_thread_stop = 0;
static int g_main_thread_stop = 0;
static int g_sockfd;
static struct sockaddr_in g_server;
static char *g_server_str = NULL;
static int g_port = 20202;

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

int get_disks(struct disk_info **di, int *nr)
{
    char *line = NULL;
    size_t len = 0;
    FILE *f = popen(GET_SCSI_DISK_SN, "r");
    int err = 0, lnr = 0;

    if (f == NULL) {
        err = -errno;
        hvfs_err(lib, "popen(GET_SCSI_DISK_SN) failed w/ %s\n",
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

int get_disk_parts(struct disk_part_info **dpi, int *nr)
{
    char *line = NULL;
    size_t len = 0;
    FILE *f = popen(GET_SCSI_DISKPART_SN, "r");
    int err = 0, lnr = 0;

    if (f == NULL) {
        err = -errno;
        hvfs_err(lib, "popen(GET_SCSI_DISKPART_SN) failed w/ %s\n",
                 strerror(errno));
        goto out;
    } else {
        *dpi = NULL;
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
                        (*dpi)[*nr - 1].read_nr = 0;
                        (*dpi)[*nr - 1].write_nr = 0;
                        (*dpi)[*nr - 1].err_nr = 0;
                        (*dpi)[*nr - 1].used = 0;
                        (*dpi)[*nr - 1].free = 0;
                        break;
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
            } else {
                t[i].used = s.f_bsize * (s.f_blocks - s.f_bavail);
                t[i].free = s.f_bsize * s.f_bavail;
            }
        }
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
    for (i = 0; i < nr; i++) {
        n += sprintf(buf + n, "%s:%s,%ld,%ld,%ld\n", 
                     dpi[i].dev_sn, dpi[i].mount_path,
                     dpi[i].read_nr, dpi[i].write_nr, dpi[i].err_nr);
    }
#ifdef SELF_TEST
    {
        char *str = "dev-hello:/mnt/hvfs,0,0,0\n";
        sprintf(buf + n, "%s", str);
    }
#endif

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
    hvfs_info(lib, "%s", buf);

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
        hvfs_info(lib, "recv '%s'\n", reply);
        *recv = strdup(reply);
    } else
        *recv = NULL;

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
        err = get_disk_parts(&dpi, &nr);
        if (err) {
            hvfs_err(lib, "get_disk_parts() failed w/ %d\n", err);
            goto update_last;
        }
        
        err = fix_disk_parts(dpi, nr);
        if (err) {
            hvfs_warning(lib, "fix_disk_parts() failed w/ %s(%d), ignore "
                     "any NRs\n", strerror(-err), err);
        }
        fd = open_shm(O_TRUNC);
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

void do_heartbeat(time_t cur)
{
    static time_t last = 0;
    char query[64 * 1024];
    char *recv = NULL;
    int err = 0;

    if (cur - last >= g_ds_conf.hb_interval) {
        struct disk_part_info *dpi = NULL;
        int nr = 0, i, len = 0;

        err = get_disk_parts(&dpi, &nr);
        if (err) {
            hvfs_err(lib, "get_disk_parts() failed w/ %d\n", err);
            goto update_last;
        }

        err = fix_disk_parts(dpi, nr);
        if (err) {
            hvfs_warning(lib, "fix_disk_parts() failed w/ %s(%d), ignore "
                         "any NRs\n", strerror(-err), err);
        }

        memset(query, 0, sizeof(query));
        for (i = 0; i < nr; i++) {
            len += sprintf(query + len, "%s:%s,%ld,%ld,%ld,%ld,%ld\n",
                           dpi[i].dev_sn, dpi[i].mount_path,
                           dpi[i].read_nr, dpi[i].write_nr, dpi[i].err_nr,
                           dpi[i].used, dpi[i].free);
        }
#ifdef SELF_TEST
        {
            char *str = "dev-hello:/mnt/hvfs,0,0,0,0,0\n";
            sprintf(query + len, "%s", str);
        }
#endif
        if (len == 0)
            goto update_last;

#if 1
        err = __dgram_sr(query, &recv);
        if (err) {
            hvfs_err(lib, "datagram send/recv failed w/ %s\n", 
                     strerror(-err));
        }
        
        if (recv == NULL || strncmp(recv, "+OK", 3) != 0) {
            hvfs_err(lib, "Heart beat with Invalid Response: %s\n", 
                     recv);
        }
        xfree(recv);
#else
        hvfs_plain(lib, "%s", query);
#endif
        dpi_free(dpi, nr);

    update_last:
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
               "Copyright (c) 2013 IIE, Ma Can <ml.macana@gmail.com>\n\n"
               "Arguments:\n"
               "-r, --server      DiskManager server IP address.\n"
               "-p, --port        UDP port of DiskManager.\n"
               "-h, -?, -help     print this help.\n"
        );
}

int main(int argc, char *argv[])
{
    struct disk_info *di = NULL;
    struct disk_part_info *dpi = NULL;
    int nr = 0, nr2 = 0;
    int err = 0;

    char *shortflags = "r:p:h?";
    struct option longflags[] = {
        {"server", required_argument, 0, 'r'},
        {"port", required_argument, 0, 'p'},
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
        default:
            hvfs_err(lib, "Invalid arguments!\n");
            return EINVAL;
        }
    }

    sem_init(&g_main_sem, 0, 0);

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

    get_disks(&di, &nr);
    di_free(di, nr);
    
    get_disk_parts(&dpi, &nr2);

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
    g_timer_thread_stop = 1;
    if (g_timer_thread) {
        sem_post(&g_timer_sem);
        pthread_join(g_timer_thread, NULL);
    }

    close(g_sockfd);

    hvfs_info(lib, "Main thread exiting ...\n");

out_timers:
out_dgram:
out_signal:
    return err;
}
