/**
 * Copyright (c) 2015 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-08-03 17:53:15 macan>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <dirent.h>

void readdir_unlink_test(int nr)
{
    struct dirent *de;
    DIR *d;
    char fname[256];
    int i;

    mkdir("./_TEST_DIR_RUT", 0755);
    for(i = 0; i < nr; i++) {
        sprintf(fname, "./_TEST_DIR_RUT/%d", i);
        FILE *f = fopen(fname, "w");
        if (!f) perror("fopen");
        fclose(f);
    }

    d = opendir("./_TEST_DIR_RUT");
    if (d == NULL) {
        perror("opendir");
        return;
    }
    i = 0;
    while (1) {
        de = readdir(d);
        if (de) {
            sprintf(fname, "./_TEST_DIR_RUT/%s", de->d_name);
            if (unlink(fname) < 0) perror(de->d_name);
            i++;
        } else break;
    }
    if (i != nr) printf("Got %d entries, expect %d\n", i, nr);
    closedir(d);
    if (rmdir("./_TEST_DIR_RUT") < 0) perror("rmdir");
}

/* This main function is using to find the read/write bug */

int read_write_test(int drop_cache)
{
    int err = 0;
    int fd = 0;
    off_t offset;
    char buf[4096];

    fd = open("./MMFS_TEST", O_CREAT | O_TRUNC | O_RDWR,
              S_IRUSR | S_IWUSR);
    if (fd < 0)
        goto out;

    close(fd);

    fd = open("./MMFS_TEST", O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0)
        goto out;

    /* write 2B to offset 0 */
    offset = lseek(fd, 0, SEEK_SET);
    if (offset < 0)
        goto out_close;
    memset(buf, '1', 4096);
    err = write(fd, buf, 2);
    if (err < 0) {
        perror("write 1");
        goto out_close;
    } else {
        printf("written 1B '1' @ %ld w/ %d\n", offset, err);
    }

    /* write 1B to offset 10M */
    offset = lseek(fd, 10 * 1024 * 1024, SEEK_SET);
    if (offset < 0)
        goto out_close;
    memset(buf, '2', 4096);
    err = write(fd, buf, 1);
    if (err < 0) {
        perror("write 2");
        goto out_close;
    } else {
        printf("written 1B '2' @ %ld w/ %d\n", offset, err);
    }

    /* write 10B to offset 100M */
    offset = lseek(fd, 100 * 1024 * 1024, SEEK_SET);
    if (offset < 0)
        goto out_close;
    memset(buf, '3', 4096);
    err = write(fd, buf, 10);
    if (err < 0) {
        perror("write 3");
        goto out_close;
    } else {
        printf("written 10B '3' @ %ld w/ %d\n", offset, err);
    }

    /* write 100B to offset 50M */
    offset = lseek(fd, 50 * 1024 * 1024, SEEK_SET);
    if (offset < 0)
        goto out_close;
    memset(buf, '4', 4096);
    err = write(fd, buf, 100);
    if (err < 0) {
        perror("write 4");
        goto out_close;
    } else {
        printf("written 100B '4' @ %ld w/ %d\n", offset, err);
    }

    if (drop_cache) {
        close(fd);
        
        printf("wait 10 seconds ...\n");
        sleep(10);
        
        fd = open("./MMFS_TEST", O_RDWR, S_IRUSR | S_IWUSR);
        if (fd < 0)
            goto out;
    }

    /* read 4096B from offset 0 */
    offset = lseek(fd, 0, SEEK_SET);
    if (offset < 0) {
        goto out_close;
    }
    memset(buf, 0, 4096);
    err = read(fd, buf, 4096);
    if (err < 0) {
        perror("read 1");
        goto out_close;
    } else {
        printf("read 4096B @ %ld w/ %d\n", offset, err);
    }
    assert(buf[0] == '1');
    assert(buf[1] == '1');
    assert(buf[4095] == '\0');

    if (err != 4096) {
        printf("Cached read failed for this build!\n");
        err = EFAULT;
    }

    /* read 4096B from offset 0 + 10M */
    offset = lseek(fd, 10 * 1024 * 1024, SEEK_SET);
    if (offset < 0) {
        goto out_close;
    }
    memset(buf, 0, 4096);
    err = read(fd, buf, 4096);
    if (err < 0) {
        perror("read 2");
        goto out_close;
    } else {
        printf("read 4096B @ %ld w/ %d\n", offset, err);
    }
    assert(buf[0] == '2');
    assert(buf[1] == '\0');
    assert(buf[4095] == '\0');

    if (err != 4096) {
        printf("Cached read failed for this build!\n");
        err = EFAULT;
    }

    /* read 4096B from offset 0 + 100M */
    offset = lseek(fd, 100 * 1024 * 1024, SEEK_SET);
    if (offset < 0) {
        goto out_close;
    }
    memset(buf, 0, 4096);
    err = read(fd, buf, 4096);
    if (err < 0) {
        perror("read 3");
        goto out_close;
    } else {
        printf("read 4096B @ %ld w/ %d\n", offset, err);
    }
    assert(buf[0] == '3');
    assert(buf[9] == '3');
    assert(buf[10] == '\0');
    assert(buf[4095] == '\0');

    if (err != 10) {
        printf("Cached read failed for this build!\n");
        err = EFAULT;
    }

    /* read 4096B from offset 50M */
    offset = lseek(fd, 50 * 1024 * 1024, SEEK_SET);
    if (offset < 0) {
        goto out_close;
    }
    memset(buf, 0, 4096);
    err = read(fd, buf, 4096);
    if (err < 0) {
        perror("read 4");
        goto out_close;
    } else {
        printf("read 4096B @ %ld w/ %d\n", offset, err);
    }
    assert(buf[0] == '4');
    assert(buf[99] == '4');
    assert(buf[100] == '\0');
    assert(buf[4095] == '\0');

    if (err != 4096) {
        printf("Cached read failed for this build!\n");
        err = EFAULT;
    }

    /* read 4096B from offset 80M */
    offset = lseek(fd, 80 * 1024 * 1024, SEEK_SET);
    if (offset < 0) {
        goto out_close;
    }
    memset(buf, 0, 4096);
    err = read(fd, buf, 4096);
    if (err < 0) {
        perror("read X");
        goto out_close;
    } else {
        printf("read 4096B @ %ld w/ %d\n", offset, err);
    }
    assert(buf[0] == '\0');
    assert(buf[99] == '\0');
    assert(buf[100] == '\0');
    assert(buf[4095] == '\0');

    if (err != 4096) {
        printf("Cached read failed for this build!\n");
        err = EFAULT;
    }

out_unlink:
    unlink("./MMFS_TEST");

    sleep(10);
    {
        struct stat buf;

        printf("wait 10 seconds ... then do fstat\n");
        err = fstat(fd, &buf);
        if (err) {
            perror("fstat");
        }
        close(fd);
    }

out:
    return err;
out_close:
    close(fd);
    goto out_unlink;
}

int main(int argc, char *argv[])
{
    int err = 0;

    err = read_write_test(0);
    if (err) {
        printf("read_write_test w/  cache failed w/ %d\n", err);
        goto out;
    }
    err = read_write_test(1);
    if (err) {
        printf("read_write_test w/o cache failed w/ %d\n", err);
        goto out;
    }

    return err;
}
