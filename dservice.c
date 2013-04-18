/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-04-17 16:11:14 macan>
 *
 */

#include "common.h"

struct dservice_conf
{
    int hb_interval;
};

static struct dservice_conf g_ds_conf = {
    .hb_interval = 5,
};

static sem_t g_timer_sem;
static sem_t g_main_sem;
static pthread_t g_timer_thread = 0;
static int g_timer_thread_stop = 0;
static int g_main_thread_stop = 0;
static int g_sockfd;
static struct sockaddr_in g_server;

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

int write_shm(int fd, struct disk_part_info *dpi, int nr)
{
    char buf[64 * 1024];
    int i, bw, bl = 0, err = 0, n = 0;

    memset(buf, 0, sizeof(buf));
    for (i = 0; i < nr; i++) {
        n += sprintf(buf + n, "%s:%s,0,0,0\n", 
                     dpi[i].dev_sn, dpi[i].mount_path);
    }
#ifdef SELF_TEST
    {
        char *str = "dev-hello:/mnt/hvfs,0,0,0\n";
        sprintf(buf + n, "%s", str);
    }
#endif

    do {
        bw = write(fd, buf, strlen(buf));
        if (bw < 0) {
            hvfs_err(lib, "write() failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        }
        bl += bw;
    } while (bl < strlen(buf));
    err = bl;

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

void do_heartbeat(time_t cur)
{
    static time_t last = 0;
    char *query = "test_query from DS\n";
    char *recv = NULL;
    int err = 0;

    if (cur - last >= g_ds_conf.hb_interval) {
        struct disk_part_info *dpi = NULL;
        int nr = 0;

        err = get_disk_parts(&dpi, &nr);
        if (err) {
        }

        err = __dgram_sr(query, &recv);
        if (err) {
            hvfs_err(lib, "datagram send/recv failed w/ %s\n", 
                     strerror(-err));
        }
        
        if (recv == NULL || strcmp(recv, "+OK") != 0) {
            hvfs_err(lib, "Heart beat with Invalid Response: %s\n", 
                     recv);
        }
        xfree(recv);
        dpi_free(dpi, nr);
        
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

int main(int argc, char *argv[])
{
    struct disk_info *di = NULL;
    struct disk_part_info *dpi = NULL;
    int nr = 0, nr2 = 0;
    int err = 0;

    sem_init(&g_main_sem, 0, 0);

    /* setup signals */
    err = __init_signal();
    if (err) {
        hvfs_err(lib, "Init signals failed w/ %d\n", err);
        goto out_signal;
    }

    /* setup dgram */
    err = setup_dgram("localhost", 20202);
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

    int fd = open_shm(0);
    write_shm(fd, dpi, nr2);
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
