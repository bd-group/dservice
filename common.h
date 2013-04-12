/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-04-12 15:19:13 macan>
 *
 */

#ifndef __COMMON_H__
#define __COMMON_H__

#define HVFS_TRACING

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
#include "lib/lib.h"

HVFS_TRACING_INIT();

#define GET_SCSI_DISK_SN "find /dev/disk/by-id/ -iregex '.*scsi-[^-]*$' -exec stat -c '%N' {} \\; | sed -e 's/.*scsi-\\([0-9a-zA-Z_]*\\).*..\\/..\\/\\([a-z]*\\).*/OK \\1 \\2/g'"

struct disk_info
{
    char *dev_sn;               /* global unique SN */
    char *dev_id;               /* local sd* */
};

#define GET_SCSI_DISKPART_SN "find /dev/disk/by-id/ -iregex '.*scsi-.*part.*' -exec stat -c '%N' {} \\; | sed -e 's/.*scsi-\\([0-9a-zA-Z_-]*\\).*..\\/..\\/\\([a-z0-9]*\\).*/\\1 \\2/g' | sort | awk '{cmd = \"mount -l | grep \" $2; while ((cmd | getline result) > 0) {print $0\" \"result} }' | awk '{print \"OK\",$1,$2,$5}'"

struct disk_part_info
{
    char *dev_sn;               /* global unique SN-partX */
    char *dev_id;               /* local sd*X */
    char *mount_path;           /* current mount path */
};

#define DEV_MAPPING           "/DEV_MAPPING"

#define _GNU_SOURCE

#endif
