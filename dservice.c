/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-04-12 16:37:12 macan>
 *
 */

#include "common.h"

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
        hvfs_info(lib, "Exit DService ...\n");
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
    int err = 0;
    char *line = NULL;
    size_t len = 0;
    FILE *f = popen(GET_SCSI_DISK_SN, "r");

    if (f == NULL) {
        perror("popen");
    } else {
        *di = NULL;
        *nr = 0;

        /* read from the result stream */
        while ((err = getline(&line, &len, f)) != -1) {
            /* ok, parse the results */
            if (err > 2) {
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

int get_disk_parts(struct disk_part_info **dpi, int *nr)
{
    int err = 0;
    char *line = NULL;
    size_t len = 0;
    FILE *f = popen(GET_SCSI_DISKPART_SN, "r");

    if (f == NULL) {
        perror("popen");
    } else {
        *dpi = NULL;
        *nr = 0;

        /* read from the result stream */
        while ((err = getline(&line, &len, f)) != -1) {
            /* ok, parse the results */
            if (err > 2) {
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
    char *str = "hello:/mnt/data1,0,0,0";
    
    write(fd, str, strlen(str));
}

void unlink_shm()
{
    shm_unlink(DEV_MAPPING);
}

int main(int argc, char *argv[])
{
    struct disk_info *di = NULL;
    struct disk_part_info *dpi = NULL;
    int nr = 0, nr2 = 0;
    int err = 0;

    /* setup signals */
    err = __init_signal();
    if (err) {
        hvfs_err(lib, "Init signals failed w/ %d\n", err);
        goto out_signal;
    }

    get_disks(&di, &nr);

    get_disk_parts(&dpi, &nr2);

    hvfs_info(lib, "Got NR %d\n", nr);
    hvfs_info(lib, "Got NR %d\n", nr2);

    int fd = open_shm(0);
    write_shm(fd, NULL, 0);
//    unlink_shm();

out_signal:
    return err;
}
