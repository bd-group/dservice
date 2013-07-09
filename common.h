/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-07-09 11:43:48 macan>
 *
 */

#ifndef __COMMON_H__
#define __COMMON_H__

#define HVFS_TRACING

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <getopt.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include "lib/lib.h"
#include <sys/types.h>
#include <sys/wait.h>
#include <limits.h>

HVFS_TRACING_INIT();

#define GET_SCSI_DISK_SN "find /dev/disk/by-id/ -iregex '.*scsi-[^-]*$' -exec stat -c '%N' {} \\; | sed -e 's/.*scsi-\\([0-9a-zA-Z_]*\\).*..\\/..\\/\\([a-z]*\\).*/OK \\1 \\2/g'"

struct disk_info
{
    char *dev_sn;               /* global unique SN */
    char *dev_id;               /* local sd* */
};

#define GET_SCSI_DISKPART_SN "find /dev/disk/by-id/ -iregex '.*scsi-.*part.*' -exec stat -c '%N' {} \\; | sed -e 's/.*scsi-\\([0-9a-zA-Z_-]*\\).*..\\/..\\/\\([a-z0-9]*\\).*/\\1 \\2/g' | sort | awk '{cmd = \"mount -l | grep \" $2; while ((cmd | getline result) > 0) {print $0\" \"result} }' | awk 'BEGIN{FS=\" type\"} {print \"OK\",$1}' | cut -d\" \" -f1,2,3,6-"

#define GET_SCSI_DISK_SN_EXT "find /dev/disk/by-id/ -iregex '.*scsi-[^-]*$' -exec stat -c '%N' {} \\; | sed -e 's/.*scsi-\\([0-9a-zA-Z_-]*\\).*..\\/..\\/\\([a-z0-9]*\\).*/\\1 \\2/g' | sort | awk '{cmd = \"mount -l | grep \" $2; while ((cmd | getline result) > 0) {print $0\" \"result} }' | awk 'BEGIN{FS=\" type\"} {print \"OK\",$1}' | cut -d\" \" -f1,2,3,6-"

struct disk_part_info
{
    char *dev_sn;               /* global unique SN-partX */
    char *dev_id;               /* local sd*X */
    char *mount_path;           /* current mount path */
    long read_nr;
    long write_nr;
    long err_nr;
    long used;
    long free;
};

struct floc_desc
{
    char *node;
    char *devid;
    char *mp;
    char *location;
};

struct rep_args
{
    struct list_head list;
    struct floc_desc to, from;
    time_t ttl;
    char *digest;
    int retries;
#define REP_STATE_INIT          0
#define REP_STATE_DOING         1
#define REP_STATE_DONE          2
#define REP_STATE_ERROR         3
#define REP_STATE_ERROR_DONE    4
    int status;
};

struct del_args
{
    struct list_head list;
    struct floc_desc target;
    time_t ttl;
    int retries;
#define DEL_STATE_INIT          0
#define DEL_STATE_DOING         1
#define DEL_STATE_DONE          2
#define DEL_STATE_ERROR         3
#define DEL_STATE_ERROR_DONE    4
    int status;
};

#define DEV_MAPPING           "/DEV_MAPPING"

#define _GNU_SOURCE

#endif
