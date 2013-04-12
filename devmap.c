/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-04-12 16:15:16 macan>
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
    
    /* Step 1: open the shm */
    int fd = open_shm();
    if (fd < 0) {
        goto out;
    }
    
    /* Step 2: read in the content */
    memset(buf, 0, sizeof(buf));
    read(fd, buf, 4096);
    printf("buf %s\n", buf);
    close_shm(fd);
    res = buf;
out:
    content = (*env)->NewStringUTF(env, res);
    return content;
}
