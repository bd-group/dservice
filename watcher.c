/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-09-17 18:27:21 macan>
 *
 */

#define HVFS_TRACING

#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <getopt.h>
#include "lib/lib.h"
#include <inotifytools/inotifytools.h>
#include <inotifytools/inotify.h>
#include <dirent.h>
#include <libgen.h>

HVFS_TRACING_INIT();

HVFS_TRACING_DEFINE_FILE();

static pthread_t g_timer_thread = 0;
static pthread_t g_event_thread = 0;
static sem_t g_timer_sem;
static sem_t g_main_sem;
static char *g_dir = NULL;
static int g_timer_thread_stop = 0;
static int g_main_thread_stop = 0;
static unsigned int g_flags = 0;

struct watcher_config
{
    int devnr;
#define INFO_INTERVAL           5
    int info_interval;
#define DEV_PREFIX              "msdisk"
    char *dev_prefix;
};

struct watcher_config g_config = {
    .devnr = 0,
    .info_interval = INFO_INTERVAL,
    .dev_prefix = DEV_PREFIX,
};

struct opstats
{
#define OP_UNKNOWN              0
#define OP_NEW                  1
#define OP_ACCESS               2
#define OP_MODIFY               3
#define OP_CLOSE_WRITE          4
#define OP_DELETE               5
#define OP_MERGE                6
    int id;
    atomic64_t ts;              /* time stamp */
    atomic64_t new;
    atomic64_t open;
    atomic64_t close;
    atomic64_t delete;
    atomic64_t access;
    atomic64_t modify;
    atomic64_t merge;
};

static struct opstats *g_ops = NULL;

void do_help()
{
    hvfs_plain(lib, "Arguments:\n\n"
               "-d, --dir         which directory you want to WATCH.\n"
               "-i, --interval    info dump interval, default to 1 seconds.\n"
               "-r, --read        use IN_READ, would generate a lot of evnets.\n"
               "-w, --write       use IN_MODIFY, would generate a lot of evnets.\n"
               "-h, -?, -help     print this help.\n"
        );
}

static int init_opstats(int nr)
{
    int err = 0, i;

    if (nr <= 0) {
        hvfs_err(lib, "Invalid number of devices: %d\n", nr);
        err = -EINVAL;
        goto out;
    }
    g_ops = xzalloc(nr * sizeof(*g_ops));
    if (!g_ops) {
        hvfs_err(lib, "xzalloc() %d opstats failed, no memory.\n", nr);
        err = -ENOMEM;
        goto out;
    }
    for (i = 0; i < nr; i++) {
        g_ops[i].id = i + 1;
    }
    
out:
    return err;
}

static void update_opstats(char *path, int op)
{
    char *p, *save = NULL, *db = NULL, *table = NULL, *dir = NULL;
    int nr = 0, id = 0, file_change = 0;
    
    p = path;
    do {
        p = strtok_r(p, "/", &save);
        if (!p)
            break;
        nr++;
        switch (nr) {
        case 0:
            /* root entry */
            break;
        case 1:
            /* mnt */
            break;
        case 2:
        {
            /* msdisk* */
            if (strncmp(p, "msdisk", 6) != 0) {
                /* ignore this event */
                return;
            }
            id = atoi(p + 6);
            break;
        }
        case 3:
            /* data */
            if (strcmp(p, "data") != 0) {
                /* ignore this event */
                return;
            }
            break;
        case 4:
            /* database */
            if (strcmp(p, "UNNAMED-DB") == 0) {
                /* ignore this event */
                return;
            }
            db = strdup(p);
            break;
        case 5:
            /* table */
            table = strdup(p);
            break;
        case 6:
            /* dir */
            dir = strdup(p);
            break;
        default:
            /* change file */
            file_change = 1;
            break;
        }
    } while (p = NULL, 1);

    /* update opstats */
    id -= 1;
    hvfs_debug(lib, "ID %d DB %s TABLE %s DIR %s OP %d\n", id, db, table, dir, op);
    if (id >= 1 && id <= g_config.devnr) {
        switch (op) {
        case OP_NEW:
            atomic64_inc(&g_ops[id].new);
            break;
        case OP_MERGE:
            atomic64_inc(&g_ops[id].merge);
            break;
        case OP_DELETE:
            atomic64_inc(&g_ops[id].delete);
            break;
        case OP_MODIFY:
            atomic64_inc(&g_ops[id].modify);
            break;
        case OP_ACCESS:
            atomic64_inc(&g_ops[id].access);
            break;
        case OP_CLOSE_WRITE:
            atomic64_inc(&g_ops[id].close);
            break;
        default:;
        }
    }
}

static void reset_opstat()
{
    int i;
    
    for (i = 0; i < g_config.devnr; i++) {
        atomic64_set(&g_ops[i].new, 0);
        atomic64_set(&g_ops[i].merge, 0);
        atomic64_set(&g_ops[i].delete, 0);
        atomic64_set(&g_ops[i].modify, 0);
        atomic64_set(&g_ops[i].access, 0);
        atomic64_set(&g_ops[i].close, 0);
        atomic64_set(&g_ops[i].open, 0);
    }
}

char *print_opstats(time_t ts)
{
    static time_t last_ts = 0;
    char *buf = NULL;
    int offset = 0, i;

    if (ts - last_ts >= g_config.info_interval) {
        buf = xmalloc(4096);
        if (!buf) {
            hvfs_err(lib, "xmalloc() buffer failed, no memory.\n");
            return NULL;
        }
        offset += sprintf(buf + offset, "Timestamp %ld\n", ts);
        offset += sprintf(buf + offset, "    \tNEW\tDELETE\tMODIFY\tACCESS\tCLOSE\tMERGE\n");

        for (i = 0; i < g_config.devnr; i++) {
            offset += sprintf(buf + offset, "[%d]\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\n",
                              g_ops[i].id,
                              atomic64_read(&g_ops[i].new),
                              atomic64_read(&g_ops[i].delete),
                              atomic64_read(&g_ops[i].modify),
                              atomic64_read(&g_ops[i].access),
                              atomic64_read(&g_ops[i].close),
                              atomic64_read(&g_ops[i].merge)
                );
        }
        reset_opstat();
        last_ts = ts;
    }
    
    return buf;
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
    } else if (signo == SIGHUP || signo == SIGINT) {
        hvfs_info(lib, "Exit Watcher ...\n");
        g_main_thread_stop = 1;
        sem_post(&g_main_sem);
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
        {
            char *p = print_opstats(cur);
            if (p)
                hvfs_plain(lib, "%s", p);
        }
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int setup_timers(void)
{
    struct sigaction ac;
    struct itimerval value, ovalue, pvalue;
    int which = ITIMER_REAL, interval;
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
    interval = 1;
    if (interval) {
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

static void *__event_thread_main(void *arg)
{
    char path[PATH_MAX];
    struct inotify_event *ie;
    sigset_t set;
    int err;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    // set time format to 24 hour time, HH:MM:SS
    inotifytools_set_printf_timefmt( "%T" );

    /* then, we loop for the timer events */
    do {
        ie = inotifytools_next_event(-1);
        if (ie) {
            char *file = inotifytools_filename_from_wd(ie->wd);

            //inotifytools_printf(ie, "%T %w%f %e\n");
            if (file) {
                if (file[strlen(file) - 1] == '/')
                    sprintf(path, "%s%s", file, ie->name);
                else
                    sprintf(path, "%s/%s", file, ie->name);

                if (ie->mask & IN_Q_OVERFLOW) {
                    inotifytools_error();
                    continue;
                }
            
                if (ie->mask & IN_CREATE) {
                    if (ie->mask & IN_ISDIR) {
                        /* add this directory to watch list */
                        hvfs_info(lib, "Add %s to watch list\n", path);
                        err = inotifytools_watch_file(path, 
                                                      IN_CLOSE | IN_CREATE | IN_DELETE 
                                                      | IN_DELETE_SELF | g_flags);
                        if (!err) {
                            hvfs_err(lib, "add %s to watch list failed w/ %d\n",
                                     path, inotifytools_error());
                        }
                        update_opstats(path, OP_NEW);
                    } else {
                        update_opstats(path, OP_MERGE);
                    }
                } else if (ie->mask & IN_CLOSE_WRITE) {
                    //hvfs_info(lib, "file %s closed_write\n", path);
                    update_opstats(path, OP_CLOSE_WRITE);
                } else if (ie->mask & IN_DELETE || ie->mask & IN_DELETE_SELF) {
                    //hvfs_info(lib, "file %s deleted\n", path);
                    if (ie->mask & IN_ISDIR) {
                        update_opstats(path, OP_DELETE);
                    }
                }
                if (ie->mask & IN_MODIFY) {
                    //hvfs_info(lib, "file %s modified\n", path);
                    update_opstats(path, OP_MODIFY);
                }
                if (ie->mask & IN_ACCESS) {
                    hvfs_debug(lib, "file %s accessed\n", path);
                    update_opstats(path, OP_ACCESS);
                }
            }
        }
    } while (1);

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int setup_inotify(char *top_dir)
{
    char _dir[PATH_MAX] = {0,}, *full_dir = NULL;
    int err = 0;

    if (top_dir[0] != '/') {
        if (getcwd(_dir, PATH_MAX) == NULL) {
            hvfs_err(lib, "getcwd() failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        }
        strcat(_dir, "/");
        strcat(_dir, top_dir);
        full_dir = _dir;
    } else {
        full_dir = top_dir;
    }

    if (!inotifytools_initialize() ||
        !inotifytools_watch_recursively(full_dir, 
                                        g_flags | IN_CLOSE | IN_CREATE |
                                        IN_DELETE | IN_DELETE_SELF)) {
        err = inotifytools_error();
        hvfs_err(lib, "watch dir %s failed w/ %s\n", full_dir, strerror(err));
        goto out;
    }
    err = pthread_create(&g_event_thread, NULL, &__event_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create event thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }
    g_dir = full_dir;

out:
    return err;
}

int main(int argc, char *argv[])
{
    char *shortflags = "n:d:rwi:h?g";
    struct option longflags[] = {
        {"dnr", required_argument, 0, 'n'},
        {"dir", required_argument, 0, 'd'},
        {"write", no_argument, 0, 'w'},
        {"read", no_argument, 0, 'r'},
        {"interval", required_argument, 0, 'i'},
        {"debug", no_argument, 0, 'g'},
        {"help", no_argument, 0, 'h'},
    };
    int err = 0, interval = -1;

    while (1) {
        int longindex = -1;
        int opt = getopt_long(argc, argv, shortflags, longflags, &longindex);
        if (opt == -1)
            break;
        switch (opt) {
        case 'n':
            g_config.devnr = atoi(optarg);
            break;
        case 'd':
            g_dir = strdup(optarg);
            break;
        case 'r':
            g_flags |= IN_ACCESS;
            break;
        case 'w':
            g_flags |= IN_MODIFY;
            break;
        case 'g':
            hvfs_lib_tracing_flags = 0xffffffff;
            break;
        case 'i':
            interval = atoi(optarg);
            break;
        case 'h':
        case '?':
            do_help();
            return 0;
            break;
        default:
            hvfs_err(lib, "Invalid arguments!\n");
            return EINVAL;
        }
    }

    if (!g_dir) {
        hvfs_plain(lib, "Please set the directory you want to watch!\n");
        do_help();
        err = EINVAL;
        goto out;
    }

    if (!g_config.devnr) {
        hvfs_plain(lib, "Please set the dev number\n");
        do_help();
        err = EINVAL;
        goto out;
    }

    err = init_opstats(g_config.devnr);
    if (err) {
        hvfs_err(lib, "Init opstats failed w/ %d\n", err);
        goto out;
    }
    
    /* setup signals */
    err = __init_signal();
    if (err) {
        hvfs_err(lib, "Init signals failed w/ %d\n", err);
        goto out;
    }

    /* setup inotify system */
    err = setup_inotify(g_dir);
    if (err) {
        hvfs_err(lib, "Setup inotify system failed w/ %d\n", err);
        goto out;
    }
    
    /* setup timers */
    if (interval >= 0) {
        hvfs_info(lib, "Reset info dump interval to %d second(s).\n",
                  interval);
        g_config.info_interval = interval;
    }
    err = setup_timers();
    if (err) {
        hvfs_err(lib, "Setup timers failed w/ %d\n", err);
        goto out;
    }

    /* wait loop */
    while (1) {
        sem_wait(&g_main_sem);
        if (g_main_thread_stop)
            break;
    }

    g_timer_thread_stop = 1;
    if (g_timer_thread) {
        sem_post(&g_timer_sem);
        pthread_join(g_timer_thread, NULL);
    }

    hvfs_info(lib, "Main thread exiting ...\n");
    
out:
    return err;
}
