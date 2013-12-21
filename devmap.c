/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-12-20 20:39:49 macan>
 *
 */

#include <unistd.h>
#include "common.h"
#include "devmap_DevMap.h"

static int g_valid = 0;

static int open_shm()
{
    int fd = 0, oflag = O_RDONLY;

    fd = shm_open(DEV_MAPPING, oflag, 0644);
    if (fd < 0) {
        printf("shm_open(%s) failed w/ %s\n", DEV_MAPPING, strerror(errno));
        return -1;
    }

    return fd;
}

#define SHMLOCK_UN      0
#define SHMLOCK_WR      1
#define SHMLOCK_RD      2
static int lock_shm(int fd, int type)
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
        printf("lock shm file failed w/ %s\n", strerror(errno));
    }

    return err;
}

static void close_shm(int fd)
{
    close(fd);
}

JNIEXPORT jboolean JNICALL Java_devmap_DevMap_isValid(JNIEnv *env, jclass cls)
{
    return g_valid;
}

JNIEXPORT jstring JNICALL Java_devmap_DevMap_getDevMaps(JNIEnv *env, jclass cls)
{
    jstring content;
    char *res = "";
    char buf[4096];
    int err = 0;
    
    /* Step 1: open the shm */
    int fd = open_shm();
    if (fd < 0) {
        goto out;
    }
    
    /* Step 2: read in the content */
    memset(buf, 0, sizeof(buf));
    lock_shm(fd, SHMLOCK_RD);
    err = read(fd, buf, 4096);
    lock_shm(fd, SHMLOCK_UN);
    if (err < 0) 
        printf("read in shm file error %s(%d)\n", strerror(errno), errno);
    close_shm(fd);
    res = buf;
out:
    content = (*env)->NewStringUTF(env, res);
    return content;
}
