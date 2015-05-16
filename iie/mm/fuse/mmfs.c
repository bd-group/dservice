/**
 * Copyright (c) 2015 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-05-16 12:18:55 macan>
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

#include "mmfs.h"

struct __mmfs_fuse_mgr mmfs_fuse_mgr = {.inited = 0,
                                        .namespace = "default",
};
struct mmfs_sb g_msb = {
    .name = "default",
    .root_ino = MMFS_ROOT_INO,
};

static void mmfs_update_sb(struct mmfs_sb *msb)
{
    int need_retry = 1, err = 0;

    xlock_lock(&msb->lock);
    if (msb->flags & MMFS_SB_DIRTY) {
        msb->d.space_used = msb->space_used - msb->d.space_used;
        msb->d.inode_used = msb->inode_used - msb->d.inode_used;
    retry:
        err = __mmfs_update_sb(msb);
        if (err) {
            if (err == -EINVAL) {
                if (need_retry) {
                    /* this might be version mismatch, just reget the sb and do
                     * another update */
                    u64 space_used = msb->d.space_used;
                    u64 inode_used = msb->d.inode_used;
                    
                    err = __mmfs_get_sb(msb);
                    if (err) {
                        hvfs_err(lib, "Reget superblock failed w/ %d\n", err);
                        goto out;
                    }
                    msb->d.space_used = space_used;
                    msb->d.inode_used = inode_used;
                    need_retry = 0;
                    goto retry;
                }
            }
            hvfs_err(lib, "Update superblock failed w/ %d\n", err);
        }
        hvfs_debug(lib, "Write superblock: {IU=%ld, SU=%ld} done.\n",
                   msb->inode_used, msb->space_used);
        msb->flags &= ~MMFS_SB_DIRTY;
    }
out:
    xlock_unlock(&msb->lock);
}

void mmfs_debug_mode(int enable)
{
    switch (enable) {
    case 3:
        hvfs_lib_tracing_flags = 0xffffffff;
        break;
    case 2:
        hvfs_lib_tracing_flags = 0xf0000004;
        break;
    case 1:
        hvfs_lib_tracing_flags = 0xf0000001;
        break;
    case 0:
    default:
        hvfs_lib_tracing_flags = 0xf0000000;
        break;
    }
}

/* how to detect a new version?
 *
 * the following macro compare 'a' and 'b'. if a newer than b, return true,
 * otherwise, return false.
 */
#define MDU_VERSION_COMPARE(a, b) ({                    \
            int __res = 0;                              \
            if ((u32)(a) > (u32)(b))                    \
                __res = 1;                              \
            else if (((u32)(b) - (u32)(a)) > (2 << 30)) \
                __res = 1;                              \
            __res;                                      \
        })

/* we only accept this format: "/path/to/name" */
#define SPLIT_PATHNAME(pathname, path, name) do {                       \
        int __len = strlen(pathname);                                   \
        char *__tmp = (char *)pathname + __len - 1;                     \
        while (*__tmp != '/')                                           \
            __tmp--;                                                    \
        if (__tmp == pathname) {                                        \
            path = "/";                                                 \
        } else {                                                        \
            path = pathname;                                            \
            *__tmp = '\0';                                              \
        }                                                               \
        if ((__tmp + 1) == (pathname + __len)) {                        \
            name = "";                                                  \
        } else {                                                        \
            name = __tmp + 1;                                           \
        }                                                               \
    } while (0)

/* We construct a Stat-Oneshot-Cache (SOC) to boost the performance of VFS
 * create. By saving the mdu info in SOC, we can eliminate one network rtt for
 * the stat-after-create. */
struct __mmfs_soc_mgr
{
#define MMFS_SOC_HSIZE_DEFAULT  (8192)
    struct regular_hash *ht;
    u32 hsize;
    atomic_t nr;
} mmfs_soc_mgr;

struct soc_entry
{
    struct hlist_node hlist;
    char *key;
    struct mstat ms;
};

static int __soc_init(int hsize)
{
    int i;

    if (hsize)
        mmfs_soc_mgr.hsize = hsize;
    else
        mmfs_soc_mgr.hsize = MMFS_SOC_HSIZE_DEFAULT;

    mmfs_soc_mgr.ht = xmalloc(mmfs_soc_mgr.hsize * sizeof(struct regular_hash));
    if (!mmfs_soc_mgr.ht) {
        hvfs_err(lib, "Stat Oneshot Cache(SOC) hash table init failed\n");
        return -ENOMEM;
    }

    /* init the hash table */
    for (i = 0; i < mmfs_soc_mgr.hsize; i++) {
        INIT_HLIST_HEAD(&mmfs_soc_mgr.ht[i].h);
        xlock_init(&mmfs_soc_mgr.ht[i].lock);
    }
    atomic_set(&mmfs_soc_mgr.nr, 0);

    return 0;
}

static void __soc_destroy(void)
{
    xfree(mmfs_soc_mgr.ht);
}

static inline
int __soc_hash(const char *key)
{
    return __murmurhash2_64a(key, strlen(key), 0xf467eaddaf9) %
        mmfs_soc_mgr.hsize;
}

static inline
struct soc_entry *__se_alloc(const char *key, struct mstat *ms)
{
    struct soc_entry *se;

    se = xzalloc(sizeof(*se));
    if (!se) {
        hvfs_err(lib, "xzalloc() soc_entry failed\n");
        return NULL;
    }
    se->key = strdup(key);
    se->ms = *ms;

    return se;
}

static inline
void __soc_insert(struct soc_entry *new)
{
    struct regular_hash *rh;
    struct soc_entry *se;
    struct hlist_node *pos, *n;
    int idx, found = 0;

    idx = __soc_hash(new->key);
    rh = mmfs_soc_mgr.ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(se, pos, n, &rh->h, hlist) {
        if (strcmp(new->key, se->key) == 0) {
            /* already exist, then update the mstat */
            se->ms = new->ms;
            found = 1;
            break;
        }
    }
    if (!found) {
        hlist_add_head(&new->hlist, &rh->h);
    }
    xlock_unlock(&rh->lock);
    atomic_inc(&mmfs_soc_mgr.nr);
}

static inline
struct soc_entry *__soc_lookup(const char *key)
{
    struct regular_hash *rh;
    struct soc_entry *se;
    struct hlist_node *pos, *n;
    int idx, found = 0;

    if (atomic_read(&mmfs_soc_mgr.nr) <= 0)
        return NULL;
    
    idx = __soc_hash(key);
    rh = mmfs_soc_mgr.ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(se, pos, n, &rh->h, hlist) {
        if (strcmp(se->key, key) == 0) {
            hlist_del(&se->hlist);
            atomic_dec(&mmfs_soc_mgr.nr);
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    if (found)
        return se;
    else
        return NULL;
}

/* We construct a write buffer cache to absorb user's write requests and flush
 * them as a whole to disk when the file are closed. Thus, we have
 * close-to-open consistency.
 */
size_t g_pagesize = 0;
static void *zero_page = NULL;

/* We are sure that there is no page hole! */
struct __mmfs_odc_mgr
{
#define MMFS_ODC_HSIZE_DEFAULT  (8191)
    struct regular_hash *ht;
    u32 hsize;
} mmfs_odc_mgr;

struct bhhead
{
    struct hlist_node hlist;
    struct list_head bh;
    size_t size;                /* total buffer size */
    size_t asize;               /* actually size for release use */
    size_t osize;               /* old size for last update */
    struct mstat ms;
    xrwlock_t clock;
    u64 ino;                    /* who am i? */
#define BH_CLEAN        0x00
#define BH_DIRTY        0x01
#define BH_CONFIG       0x80
    u32 flag;
    atomic_t ref;
    void *ptr;                  /* private pointer */
};

struct bh
{
    struct list_head list;
    off_t offset;               /* buffer offset */
    void *data;                 /* this is always a page */
};

static int __odc_init(int hsize)
{
    int i;

    if (hsize)
        mmfs_odc_mgr.hsize = hsize;
    else
        mmfs_odc_mgr.hsize = MMFS_ODC_HSIZE_DEFAULT;

    mmfs_odc_mgr.ht = xmalloc(mmfs_odc_mgr.hsize * sizeof(struct regular_hash));
    if (!mmfs_odc_mgr.ht) {
        hvfs_err(lib, "OpeneD Cache(ODC) hash table init failed\n");
        return -ENOMEM;
    }

    /* init the hash table */
    for (i = 0; i < mmfs_odc_mgr.hsize; i++) {
        INIT_HLIST_HEAD(&mmfs_odc_mgr.ht[i].h);
        xlock_init(&mmfs_odc_mgr.ht[i].lock);
    }

    return 0;
}

static void __odc_destroy(void)
{
    xfree(mmfs_odc_mgr.ht);
}

static inline
int __odc_hash(u64 ino)
{
    return __murmurhash2_64a(&ino, sizeof(ino), 0xfade8419edfa) %
        mmfs_odc_mgr.hsize;
}

/* Return value: 0: not really removed; 1: truely removed
 */
static inline
int __odc_remove(struct bhhead *del)
{
    struct regular_hash *rh;
    struct bhhead *bhh;
    struct hlist_node *pos, *n;
    int idx;

    idx = __odc_hash(del->ino);
    rh = mmfs_odc_mgr.ht + idx;

    idx = 0;
    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(bhh, pos, n, &rh->h, hlist) {
        if (del == bhh && del->ino == bhh->ino) {
            if (atomic_dec_return(&bhh->ref) <= 0) {
                idx = 1;
                hlist_del(&bhh->hlist);
            }
            break;
        }
    }
    xlock_unlock(&rh->lock);

    return idx;
}

static struct bhhead *__odc_insert(struct bhhead *new)
{
    struct regular_hash *rh;
    struct bhhead *bhh;
    struct hlist_node *pos, *n;
    int idx, found = 0;

    idx = __odc_hash(new->ino);
    rh = mmfs_odc_mgr.ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(bhh, pos, n, &rh->h, hlist) {
        if (new->ino == bhh->ino) {
            /* already exist */
            atomic_inc(&bhh->ref);
            found = 1;
            break;
        }
    }
    if (!found) {
        hlist_add_head(&new->hlist, &rh->h);
        bhh = new;
    }
    xlock_unlock(&rh->lock);

    return bhh;
}

/* Return value: NULL: miss; other: hit
 */
static inline
struct bhhead *__odc_lookup(u64 ino)
{
    struct regular_hash *rh;
    struct bhhead *bhh;
    struct hlist_node *n;
    int idx, found = 0;

    idx = __odc_hash(ino);
    rh = mmfs_odc_mgr.ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(bhh, n, &rh->h, hlist) {
        if (bhh->ino == ino) {
            atomic_inc(&bhh->ref);
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    if (found)
        return bhh;
    else
        return NULL;
}

static inline
void __odc_lock(struct bhhead *bhh)
{
    xrwlock_wlock(&bhh->clock);
}

static inline
void __odc_unlock(struct bhhead *bhh)
{
    xrwlock_wunlock(&bhh->clock);
}

static inline
int __mmfs_update_inode_proxy(struct mstat *ms, struct mdu_update *mu)
{
    struct bhhead *bhh = NULL;
    int err = 0;

    bhh = __odc_lookup(ms->ino);
    if (bhh) {
        __odc_lock(bhh);
        if (ms->mdu.version != bhh->ms.mdu.version)
            ms->mdu = bhh->ms.mdu;
        err = __mmfs_update_inode(ms, mu);
        __odc_unlock(bhh);
        if (err == -EAGAIN) {
            __odc_lock(bhh);
            ms->mdu = bhh->ms.mdu;
            err = __mmfs_update_inode(ms, mu);
            __odc_unlock(bhh);
        }
    } else {
        err = __mmfs_update_inode(ms, mu);
    }

    return err;
}

static inline
struct bhhead* __get_bhhead(struct mstat *ms)
{
    struct bhhead *bhh, *tmp_bhh;

    bhh = __odc_lookup(ms->ino);
    if (!bhh) {
        /* create it now */
        bhh = xzalloc(sizeof(struct bhhead));
        if (unlikely(!bhh)) {
            return NULL;
        }
        INIT_LIST_HEAD(&bhh->bh);
        xrwlock_init(&bhh->clock);
        bhh->ms = *ms;
        bhh->ino = ms->ino;
        bhh->asize = ms->mdu.size;
        bhh->osize = ms->mdu.size;
        atomic_set(&bhh->ref, 1);

        /* try to insert into the table */
        tmp_bhh = __odc_insert(bhh);
        if (tmp_bhh != bhh) {
            /* someone ahead me, free myself */
            xfree(bhh);
            bhh = tmp_bhh;
        }
    }

    return bhh;
}

static inline void __set_bhh_dirty(struct bhhead *bhh)
{
    bhh->flag |= BH_DIRTY;
}
static inline void __clr_bhh_dirty(struct bhhead *bhh)
{
    bhh->flag &= ~BH_DIRTY;
}

static inline void __set_bhh_config(struct bhhead *bhh)
{
    bhh->flag = BH_CONFIG;
}

static int __prepare_bh(struct bh *bh, int alloc)
{
    if (!bh->data || bh->data == zero_page) {
        if (alloc) {
            bh->data = xzalloc(g_pagesize);
            if (!bh->data) {
                return -ENOMEM;
            }
        } else
            bh->data = zero_page;
    }

    return 0;
}

static struct bh* __get_bh(off_t off, int alloc)
{
    struct bh *bh;

    bh = xzalloc(sizeof(struct bh));
    if (!bh) {
        return NULL;
    }
    INIT_LIST_HEAD(&bh->list);
    bh->offset = off;
    if (__prepare_bh(bh, alloc)) {
        xfree(bh);
        bh = NULL;
    }

    return bh;
}

static void __put_bh(struct bh *bh)
{
    if (bh->data && bh->data != zero_page)
        xfree(bh->data);

    xfree(bh);
}

static void __put_bhhead(struct bhhead *bhh)
{
    struct bh *bh, *n;

    if (__odc_remove(bhh)) {
        list_for_each_entry_safe(bh, n, &bhh->bh, list) {
            list_del(&bh->list);
            __put_bh(bh);
        }

        xfree(bhh);
    }
}

void __odc_update(struct mstat *ms)
{
    struct bhhead *bhh = __odc_lookup(ms->ino);

    if (bhh) {
        if (MDU_VERSION_COMPARE(ms->mdu.version, bhh->ms.mdu.version)) {
            /* FIXME: this means that server's mdu has been updated. We
             * should clean up the bh cache here! */
            bhh->ms.mdu = ms->mdu;
            bhh->asize = ms->mdu.size;
        } else {
            ms->mdu = bhh->ms.mdu;
            ms->mdu.size = bhh->asize;
        }
        __put_bhhead(bhh);
    }
}

/* __bh_fill() will fill the buffer cache w/ buf. if there are holes, it will
 * fill them automatically.
 */
static int __bh_fill(struct mstat *ms, struct bhhead *bhh, 
                     void *buf, off_t offset, size_t size, int update)
{
    /* round down the offset */
    struct bh *bh;
    off_t off_end = PAGE_ROUNDUP((offset + size), g_pagesize);
    off_t loff = 0;
    ssize_t rlen;
    size_t _size = 0;
    int err = 0;

    hvfs_debug(lib, "__bh_fill(%ld) offset=%ld size=%ld bhh->size=%ld bhh->asize=%ld\n",
               ms->ino, (u64)offset, (u64)size, bhh->size, bhh->asize);
    xrwlock_wlock(&bhh->clock);
    /* should we loadin the middle holes */
    if (offset >= bhh->size) {
        while (bhh->size < off_end) {
            bh = __get_bh(bhh->size, 0);
            if (!bh) {
                err = -ENOMEM;
                goto out;
            }
            if (offset == bhh->size && size >= g_pagesize) {
                /* just copy the buffer, prepare true page */
                __prepare_bh(bh, 1);
                _size = min(size, bh->offset + g_pagesize - offset);
                if (buf)
                    memcpy(bh->data + offset - bh->offset,
                           buf + loff, _size);
                size -= _size;
                loff += _size;
                offset = bh->offset + g_pagesize;
            } else {
                /* read in the page now */
                if (bhh->size <= ms->mdu.size) {
                    __prepare_bh(bh, 1);
                }
                rlen = __mmfs_fread(ms, &bh->data, bhh->size, g_pagesize);
                if (rlen == -EFBIG) {
                    /* it is ok, we just zero the page */
                    err = 0;
                } else if (rlen < 0) {
                    hvfs_err(lib, "bh_fill() read the file range [%ld, %ld] "
                             "failed w/ %ld\n",
                             bhh->size, bhh->size + g_pagesize, rlen);
                    err = rlen;
                    goto out;
                }
                /* should we fill with buf? */
                if (size && offset < bh->offset + g_pagesize) {
                    __prepare_bh(bh, 1);
                    _size = min(size, bh->offset + g_pagesize - offset);
                    if (buf)
                        memcpy(bh->data + offset - bh->offset,
                               buf + loff, _size);
                    size -= _size;
                    loff += _size;
                    offset = bh->offset + g_pagesize;
                }
            }
            list_add_tail(&bh->list, &bhh->bh);
            bhh->size += g_pagesize;
        }
    } else {
        /* update the cached content */
        list_for_each_entry(bh, &bhh->bh, list) {
            if (offset >= bh->offset && offset < bh->offset + g_pagesize) {
                __prepare_bh(bh, 1);
                _size = min(size, bh->offset + g_pagesize - offset);
                if (buf)
                    memcpy(bh->data + offset - bh->offset,
                           buf + loff, _size);
                size -= _size;
                loff += _size;
                offset = bh->offset + g_pagesize;
                if (size <= 0)
                    break;
            }
        }
        if (size) {
            /* fill the last holes */
            while (bhh->size < off_end) {
                bh = __get_bh(bhh->size, 1);
                if (!bh) {
                    err = -ENOMEM;
                    goto out;
                }
                if (offset == bhh->size && size >= g_pagesize) {
                    /* just copy the buffer */
                    _size = min(size, bh->offset + g_pagesize - offset);
                    if (buf)
                        memcpy(bh->data + offset - bh->offset,
                               buf + loff, _size);
                    size -= _size;
                    loff += _size;
                    offset = bh->offset + g_pagesize;
                } else {
                    /* read in the page now */
                    rlen = __mmfs_fread(ms, &bh->data, bhh->size, g_pagesize);
                    if (rlen == -EFBIG) {
                        /* it is ok, we just zero the page */
                        err = 0;
                    } else if (rlen < 0) {
                        hvfs_err(lib, "bh_fill() read the file range [%ld, %ld] "
                                 "failed w/ %ld",
                                 bhh->size, bhh->size + g_pagesize, rlen);
                        err = rlen;
                        goto out;
                    }
                    /* should we fill with buf? */
                    if (size && offset < bh->offset + g_pagesize) {
                        _size = min(size, bh->offset + g_pagesize - offset);
                        if (buf)
                            memcpy(bh->data + offset - bh->offset,
                                   buf + loff, _size);
                        size -= _size;
                        loff += _size;
                        offset = bh->offset + g_pagesize;
                    }
                }
                list_add_tail(&bh->list, &bhh->bh);
                bhh->size += g_pagesize;
            }
        }
    }

out:
    xrwlock_wunlock(&bhh->clock);
    
    return err;
}

/* Return the cached bytes we can read or minus errno
 */
static int __bh_read(struct bhhead *bhh, void *buf, off_t offset, 
                     size_t size)
{
    struct bh *bh;
    off_t loff = 0, saved_offset = offset;
    size_t _size, saved_size = size;
    
    if (offset + size > bhh->size || list_empty(&bhh->bh)) {
        return -EFBIG;
    }
    
    xrwlock_rlock(&bhh->clock);
    list_for_each_entry(bh, &bhh->bh, list) {
        if (offset >= bh->offset && offset < bh->offset + g_pagesize) {
            _size = min(size, bh->offset + g_pagesize - offset);
            memcpy(buf + loff, bh->data + offset - bh->offset,
                   _size);
            /* adjust the offset and size */
            size -= _size;
            loff += _size;
            offset = bh->offset + g_pagesize;
            if (size <= 0)
                break;
        }
    }
    xrwlock_runlock(&bhh->clock);

    size = saved_size - size;
    /* adjust the return size to valid file range */
    if (saved_offset + size > bhh->asize) {
        size = bhh->asize - saved_offset;
        if ((ssize_t)size < 0)
            size = 0;
    }
    
    return size;
}

static int __bh_sync(struct bhhead *bhh)
{
    struct mstat ms;
    struct bh *bh;
    struct iovec *iov = NULL;
    off_t offset = 0;
    void *data = NULL;
    size_t size, _size;
    int err = 0, i;

    ms = bhh->ms;

    if (bhh->asize > bhh->size) {
        /* oh, we have to fill the remain pages */
        err = __bh_fill(&ms, bhh, NULL, bhh->asize, 0, 1);
        if (err < 0) {
            hvfs_err(lib, "fill the buffer cache failed w/ %d\n",
                     err);
            goto out;
        }
        ms.pino = bhh->ms.pino;
    }

    hvfs_debug(lib, "__bh_sync() size=%ld asize %ld mdu.size %ld\n",
               bhh->size, bhh->asize, bhh->ms.mdu.size);
    xrwlock_wlock(&bhh->clock);
    size = bhh->asize;
    i = 0;
    list_for_each_entry(bh, &bhh->bh, list) {
        _size = min(size, g_pagesize);
        i++;
        size -= _size;
        if (size <= 0)
            break;
    }

    if (i > IOV_MAX - 5) {
        /* sadly fallback to memcpy approach */
        data = xmalloc(bhh->asize);
        if (!data) {
            hvfs_err(lib, "xmalloc(%ld) data buffer failed\n", 
                     bhh->asize);
            xrwlock_wunlock(&bhh->clock);
            return -ENOMEM;
        }

        size = bhh->asize;
        list_for_each_entry(bh, &bhh->bh, list) {
            _size = min(size, g_pagesize);
            memcpy(data + offset, bh->data, _size);
            offset += _size;
            size -= _size;
            if (size <= 0)
                break;
        }
    } else {
        iov = xmalloc(sizeof(*iov) * i);
        if (!iov) {
            hvfs_err(lib, "xmalloc() iov buffer failed\n");
            xrwlock_wunlock(&bhh->clock);
            return -ENOMEM;
        }
        
        size = bhh->asize;
        i = 0;
        list_for_each_entry(bh, &bhh->bh, list) {
            _size = min(size, g_pagesize);
            
            (iov + i)->iov_base = bh->data;
            (iov + i)->iov_len = _size;
            i++;
            size -= _size;
            if (size <= 0)
                break;
        }
    }
    __clr_bhh_dirty(bhh);
    xrwlock_wunlock(&bhh->clock);

    /* write out the data now */
    if (data) {
        err = __mmfs_fwrite(&ms, 0, data, bhh->asize);
        if (err) {
            hvfs_err(lib, "do internal fwrite on ino'%lx' failed w/ %d\n",
                     ms.ino, err);
            goto out_free;
        }
    } else {
        err = __mmfs_fwritev(&ms, 0, iov, i);
        if (err) {
            hvfs_err(lib, "do internal fwrite on ino'%lx' failed w/ %d\n",
                     ms.ino, err);
            goto out_free;
        }
    }

    /* update the file attributes */
    {
        struct mdu_update mu = {0,};

        mu.valid = MU_SIZE | MU_MTIME;
        mu.size = bhh->asize;
        mu.mtime = time(NULL);

        err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err) {
            hvfs_err(lib, "do internal update on ino<%lx> failed w/ %d\n",
                     ms.ino, err);
            goto out_free;
        }
        __update_msb(MMFS_SB_U_SPACE, mu.size - bhh->osize);
        bhh->osize = mu.size;
        /* finally, update bhh->hs */
        bhh->ms = ms;
    }

    err = size;

out_free:
    xfree(iov);
    xfree(data);

out:
    return err;
}

/* We have a LRU translate cache to resolve file system pathname(only
 * directory) to ino.
 */
static time_t *g_mmfs_tick = NULL; /* file system tick */
struct __mmfs_ltc_mgr
{
    struct regular_hash *ht;
    struct list_head lru;
    xlock_t lru_lock;
#define MMFS_LTC_HSIZE_DEFAULT  (8191)
    u32 hsize:16;               /* hash table size */
    u32 ttl:8;                  /* valid ttl. 0 means do not believe the
                                 * cached value (cache disabled) */
} mmfs_ltc_mgr;

struct ltc_entry
{
    struct hlist_node hlist;
    struct list_head list;
    char *fullname;             /* full pathname */
    u64 ino;
    u64 born;
    u32 mdu_flags;
};

static int __ltc_init(int ttl, int hsize)
{
    int i;
    
    /* init file system tick */
    g_mmfs_tick = &mmfs_fuse_mgr.tick;

    if (hsize)
        mmfs_ltc_mgr.hsize = hsize;
    else
        mmfs_ltc_mgr.hsize = MMFS_LTC_HSIZE_DEFAULT;

    mmfs_ltc_mgr.ttl = ttl;

    mmfs_ltc_mgr.ht = xmalloc(mmfs_ltc_mgr.hsize * sizeof(struct regular_hash));
    if (!mmfs_ltc_mgr.ht) {
        hvfs_err(lib, "LRU Translate Cache hash table init failed\n");
        return -ENOMEM;
    }

    /* init the hash table */
    for (i = 0; i < mmfs_ltc_mgr.hsize; i++) {
        INIT_HLIST_HEAD(&mmfs_ltc_mgr.ht[i].h);
        xlock_init(&mmfs_ltc_mgr.ht[i].lock);
    }
    INIT_LIST_HEAD(&mmfs_ltc_mgr.lru);
    xlock_init(&mmfs_ltc_mgr.lru_lock);

    return 0;
}

static void __ltc_destroy(void)
{
    xfree(mmfs_ltc_mgr.ht);
}

#define LE_LIFE_FACTOR          (4)
#define LE_IS_OLD(le) (                                                 \
        ((*g_mmfs_tick - (le)->born) >                                  \
         LE_LIFE_FACTOR * mmfs_ltc_mgr.ttl)                             \
        )
#define LE_IS_VALID(le) (*g_mmfs_tick - (le)->born <= mmfs_ltc_mgr.ttl)

static inline
int __ltc_hash(const char *key)
{
    return __murmurhash2_64a(key, strlen(key), 0xfead31435df3) % 
        mmfs_ltc_mgr.hsize;
}

static void __ltc_remove(struct ltc_entry *del)
{
    struct regular_hash *rh;
    struct ltc_entry *le;
    struct hlist_node *pos, *n;
    int idx;

    idx = __ltc_hash(del->fullname);
    rh = mmfs_ltc_mgr.ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(le, pos, n, &rh->h, hlist) {
        if (del == le && strcmp(del->fullname, le->fullname) == 0) {
            hlist_del(&le->hlist);
            break;
        }
    }
    xlock_unlock(&rh->lock);
}

static struct ltc_entry *
__ltc_new_entry(char *pathname, void *arg0, void *arg1)
{
    struct ltc_entry *le = NULL;

    /* find the least recently used entry */
    if (!list_empty(&mmfs_ltc_mgr.lru)) {
        xlock_lock(&mmfs_ltc_mgr.lru_lock);
        le = list_entry(mmfs_ltc_mgr.lru.prev, struct ltc_entry, list);
        /* if it is born long time ago, we reuse it! */
        if (LE_IS_OLD(le)) {
            /* remove from the tail */
            list_del_init(&le->list);

            xlock_unlock(&mmfs_ltc_mgr.lru_lock);
            /* remove from the hash table */
            __ltc_remove(le);

            /* install new values */
            xfree(le->fullname);
            le->fullname = strdup(pathname);
            if (!le->fullname) {
                /* failed with not enough memory! */
                xfree(le);
                le = NULL;
                goto out;
            }
            le->ino = (u64)arg0;
            le->mdu_flags = (u32)(u64)arg1;
            le->born = *g_mmfs_tick;
        } else {
            xlock_unlock(&mmfs_ltc_mgr.lru_lock);
            goto alloc_one;
        }
    } else {
    alloc_one:
        le = xmalloc(sizeof(*le));
        if (!le) {
            goto out;
        }
        le->fullname = strdup(pathname);
        if (!le->fullname) {
            xfree(le);
            le = NULL;
            goto out;
        }
        le->ino = (u64)arg0;
        le->mdu_flags = (u32)(u64)arg1;
        le->born = *g_mmfs_tick;
    }

out:
    return le;
}

/* Return value: 1 => hit and up2date; 2 => miss, alloc and up2date; 
 *               0 => not up2date
 */
static int __ltc_update(char *pathname, void *arg0, void *arg1)
{
    struct regular_hash *rh;
    struct ltc_entry *le;
    struct hlist_node *n;
    int found = 0, idx;

    /* ABI: arg0, and arg1 is ino and mdu_flags */
    idx = __ltc_hash(pathname);
    rh = mmfs_ltc_mgr.ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(le, n, &rh->h, hlist) {
        if (strcmp(le->fullname, pathname) == 0) {
            /* ok, we update the entry */
            le->ino = (u64)arg0;
            le->mdu_flags = (u32)(u64)arg1;
            le->born = *g_mmfs_tick;
            found = 1;
            /* move to the head of lru list */
            xlock_lock(&mmfs_ltc_mgr.lru_lock);
            list_del_init(&le->list);
            list_add(&le->list, &mmfs_ltc_mgr.lru);
            xlock_unlock(&mmfs_ltc_mgr.lru_lock);
            break;
        }
    }
    if (unlikely(!found)) {
        le = __ltc_new_entry(pathname, arg0, arg1);
        if (likely(le)) {
            found = 2;
        }
        /* insert to this hash list */
        hlist_add_head(&le->hlist, &rh->h);
        /* insert to the lru list */
        xlock_lock(&mmfs_ltc_mgr.lru_lock);
        list_add(&le->list, &mmfs_ltc_mgr.lru);
        xlock_unlock(&mmfs_ltc_mgr.lru_lock);
    }
    xlock_unlock(&rh->lock);
    
    return found;
}

/* Return value: 0: miss; 1: hit; <0: error
 */
static inline
int __ltc_lookup(char *pathname, void *arg0, void *arg1)
{
    struct regular_hash *rh;
    struct ltc_entry *le;
    struct hlist_node *n;
    int found = 0, idx;

    idx = __ltc_hash(pathname);
    rh = mmfs_ltc_mgr.ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(le, n, &rh->h, hlist) {
        if (LE_IS_VALID(le) && 
            strcmp(pathname, le->fullname) == 0
            ) {
            *(u64 *)arg0 = le->ino;
            *(u32 *)arg1 = le->mdu_flags;
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    return found;
}

static inline
void __ltc_invalid(const char *pathname)
{
    struct regular_hash *rh;
    struct ltc_entry *le;
    struct hlist_node *pos, *n;
    int idx;

    idx = __ltc_hash(pathname);
    rh = mmfs_ltc_mgr.ht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry_safe(le, pos, n, &rh->h, hlist) {
        if (strcmp(pathname, le->fullname) == 0) {
            le->born -= mmfs_ltc_mgr.ttl;
            break;
        }
    }
    xlock_unlock(&rh->lock);
}

/* GETATTR: use cclient to send request to server
 */
int mmfs_getattr(const char *pathname, struct stat *stbuf)
{
    struct mstat ms = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    {
        struct soc_entry *se = __soc_lookup(pathname);

        if (unlikely(se)) {
            ms = se->ms;
            xfree(se);
            goto pack;
        }
    }

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    /* lookup the file in the parent directory now */
    if (strlen(name) > 0) {
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_debug(lib, "do internal file stat on '%s'"
                       " failed w/ %d pino %lx (RT %lx)\n",
                       name, err, pino, g_msb.root_ino);
            goto out;
        }
    } else {
        /* check if it is the root directory */
        if (pino == g_msb.root_ino) {
            /* stat root w/o any file name, it is ROOT we want to stat */
            err = __mmfs_fill_root(&ms);
            if (err) {
                hvfs_err(lib, "fill root entry failed w/ %d\n", err);
                goto out;
            }
        }
    }

    /* update ms w/ local ODC cached mstat */
    {
        struct bhhead *bhh = __odc_lookup(ms.ino);

        if (unlikely(bhh)) {
            hvfs_debug(lib, "1. ODC update size? v%d,%d, bhh->asize=%ld, mdu.size=%ld\n",
                       ms.mdu.version, bhh->ms.mdu.version, bhh->asize, ms.mdu.size);
            if (MDU_VERSION_COMPARE(ms.mdu.version, bhh->ms.mdu.version)) {
                /* FIXME: this means that server's mdu has been updated. We
                 * should clean up the bh cache here! */
                bhh->ms.mdu = ms.mdu;
                bhh->asize = ms.mdu.size;
            } else {
                ms.mdu = bhh->ms.mdu;
                ms.mdu.size = bhh->asize;
            }
            __put_bhhead(bhh);
            hvfs_debug(lib, "2. ODC update size? v%d,%d, bhh->asize=%ld, mdu.size=%ld\n",
                       ms.mdu.version, bhh->ms.mdu.version, bhh->asize, ms.mdu.size);
        }
    }

pack:
    /* pack the result to stat buffer */
    stbuf->st_ino = ms.ino;
    stbuf->st_mode = ms.mdu.mode;
    stbuf->st_rdev = ms.mdu.dev;
    stbuf->st_nlink = ms.mdu.nlink;
    stbuf->st_uid = ms.mdu.uid;
    stbuf->st_gid = ms.mdu.gid;
    stbuf->st_ctime = (time_t)ms.mdu.ctime;
    stbuf->st_atime = (time_t)ms.mdu.atime;
    stbuf->st_mtime = (time_t)ms.mdu.mtime;
    if (unlikely(S_ISDIR(ms.mdu.mode))) {
        stbuf->st_size = 0;
        stbuf->st_blocks = 1;
    } else {
        stbuf->st_size = ms.mdu.size;
        stbuf->st_blocks = (ms.mdu.size + 511) >> 9;
    }
    stbuf->st_blksize = 4096;
    
out:
    xfree(dup);
    xfree(spath);
    
    return err;
}

static int mmfs_readlink(const char *pathname, char *buf, size_t size)
{
    struct mstat ms = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    /* lookup the file in the parent directory now */
    if (name && strlen(name) > 0 && strcmp(name, "/") != 0) {
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
    } else {
        hvfs_err(lib, "Readlink from a directory is not allowed.\n");
        err = -EINVAL;
        goto out;
    }

    /* ok to parse the symname */
    {
        err = __mmfs_readlink(pino, &ms);
        if (err) {
            hvfs_err(lib, "readlink on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        memset(buf, 0, size);
        memcpy(buf, ms.arg, min(ms.mdu.size, size));
    }

out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_mknod(const char *pathname, mode_t mode, dev_t rdev)
{
    struct mstat ms;
    struct mdu_update mu = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    /* create the file or dir in the parent directory now */
    ms.name = name;
    ms.ino = 0;

    mu.valid = MU_MODE | MU_DEV;
    mu.mode = mode;
    mu.dev = rdev;
    err = __mmfs_create(pino, &ms, &mu, __MMFS_CREATE_ALL);
    if (err) {
        hvfs_err(lib, "do internal create on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }

out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_mkdir(const char *pathname, mode_t mode)
{
    struct mstat ms = {0,}, pms = {0,};
    struct mdu_update mu;
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
        pms = ms;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
    mdu_flags = (u32)ms.mdu.flags;
hit:
    /* create the file or dir in the parent directory now */
    ms.name = name;
    ms.ino = 0;
    mu.valid = MU_MODE | MU_ATIME | MU_CTIME | MU_MTIME;
    mu.mode = mode | S_IFDIR;
    mu.atime = mu.ctime = mu.mtime = time(NULL);
    ms.mdu.flags |= MMFS_MDU_DIR;

    err = __mmfs_create(pino, &ms, &mu, __MMFS_CREATE_DIR | __MMFS_CREATE_ALL);
    if (err) {
        hvfs_err(lib, "do internal create on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }

    __ltc_update((char *)pathname, (void *)ms.ino, (void *)(u64)ms.mdu.flags);

    mu.valid = MU_NLINK_DELTA;
    mu.nlink = 1;
    if (pms.ino == 0) {
        /* stat it */
        pms.ino = pino;
        err = __mmfs_stat(0, &pms);
        if (err) {
            hvfs_err(lib, "do internal stat on _IN_%ld failed w/ %d\n",
                     pino, err);
            goto out;
        }
    }
    err = __mmfs_update_inode(&pms, &mu);
    if (err) {
        hvfs_err(lib, "do internal update on _IN_%ld failed w/ %d\n",
                 pms.ino, err);
        goto out;
    }

out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_unlink(const char *pathname)
{
    struct mstat ms = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    /* finally, do delete now */
    ms.name = name;
    ms.ino = 0;
    err = __mmfs_unlink(pino, &ms, __MMFS_UNLINK_ALL);
    if (err) {
        hvfs_err(lib, "do internal delete on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }
    if (!S_ISLNK(ms.mdu.mode))
        __update_msb(MMFS_SB_U_SPACE, -ms.mdu.size);

out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_rmdir(const char *pathname)
{
    struct mstat ms = {0,}, pms = {0,};
    struct mdu_update mu;
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
        pms = ms;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    /* finally, do delete now */
    if (strlen(name) == 0 || strcmp(name, "/") == 0) {
        /* what we want to delete is the root directory, reject it */
        hvfs_err(lib, "Reject root directory removal!\n");
        err = -ENOTEMPTY;
        goto out;
    } else {
        /* confirm what it is firstly! */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        if (!S_ISDIR(ms.mdu.mode)) {
            hvfs_err(lib, "not a directory, we expect dir here.\n");
            err = -ENOTDIR;
            goto out;
        }
        /* is this directory empty */
        if (!__mmfs_is_empty_dir(ms.ino)) {
            err = -ENOTEMPTY;
            goto out;
        }
        /* delete a normal file or dir, it is easy */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_unlink(pino, &ms, __MMFS_UNLINK_ALL);
        if (err) {
            hvfs_err(lib, "do internal delete on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        __ltc_invalid(pathname);
        /* revert parent directory's nlink */
        mu.valid = MU_NLINK_DELTA;
        mu.nlink = -1;
        if (pms.ino == 0) {
            /* stat it */
            pms.ino = pino;
            err = __mmfs_stat(0, &pms);
            if (err) {
                hvfs_err(lib, "do internal stat on _IN_%ld failed w/ %d\n",
                         pino, err);
                goto out;
            }
        }
        err = __mmfs_update_inode(&pms, &mu);
        if (err) {
            hvfs_err(lib, "do internal update on _IN_%ld failed w/ %d\n",
                     pms.ino, err);
            goto out;
        }

        /* delete the MMServer space */
        {
            char set[256];

            sprintf(set, "o%ld", ms.ino);
            err = mmcc_del_set(set);
            if (err) {
                hvfs_err(lib, "do MMCC set %s delete failed, manual delete.\n",
                         set);
                goto out;
            }
            hvfs_debug(lib, "MMCC set %s deleted.\n", set);
        }
    }
out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_symlink(const char *from, const char *to)
{
    struct mstat ms = {0,};
    struct mdu_update mu;
    char *dup = strdup(to), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    /* create the file or dir in the parent directory now */
    if (strlen(name) == 0 || strcmp(name, "/") == 0) {
        hvfs_err(lib, "Create zero-length named file or root directory?\n");
        err = -EINVAL;
        goto out;
    }

    ms.name = name;
    ms.ino = 0;
    ms.arg = (void *)from;
    mu.valid = MU_SYMNAME | MU_SIZE | MU_FLAG_ADD | MU_MODE | 
        MU_ATIME | MU_CTIME | MU_MTIME;
    mu.flags |= MMFS_MDU_SYMLINK;
    mu.size = strlen(from);
    mu.mode = MMFS_DEFAULT_UMASK | S_IFLNK;
    mu.atime = mu.ctime = mu.mtime = time(NULL);
    
    err = __mmfs_create(pino, &ms, &mu, __MMFS_CREATE_SYMLINK | __MMFS_CREATE_ALL);
    if (err) {
        hvfs_err(lib, "do internal create on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }
out:
    xfree(dup);
    xfree(spath);
    
    return err;
}

/* Rational for (atomic) rename:
 *
 * Basically, we stat and copy the file info to target location; and finally,
 * unlink the original entry.
 */
static int mmfs_rename(const char *from, const char *to)
{
    struct mstat ms, saved_ms, deleted_ms = {.ino = 0, .mdu.mode = 0,};
    char *dup = strdup(from), *dup2 = strdup(from),
        *path, *name, *spath = NULL, *sname;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags;
    int err = 0, isaved = 0;

    /* Step 1: get the stat info of 'from' file */
    path = dirname(dup);
    name = basename(dup2);
    sname = strdup(name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    if (name && strlen(name) > 0 && strcmp(name, "/") != 0) {
        /* we have to lookup this file now. Otherwise, what we want to lookup
         * is the last directory, just return a result string now */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        if (ms.mdu.flags & MMFS_MDU_SYMLINK) {
            err = __mmfs_readlink(ms.pino, &ms);
            if (err) {
                hvfs_err(lib, "do internal stat(SYMLINK) on '%s' "
                         "failed w/ %d\n",
                         name, err);
                goto out;
            }
        }
    } else {
        /* rename directory, it is ok */
        if (!S_ISDIR(ms.mdu.mode) || ms.ino == g_msb.root_ino) {
            hvfs_err(lib, "directory or not-directory, it is a question!\n");
            err = -EPERM;
            goto out;
        }
    }

    /* if the source file has been opened, we should use the latest mstat info
     * cached on it */
    {
        struct bhhead *bhh = __odc_lookup(ms.ino);

        if (bhh) {
            ms = bhh->ms;
            ms.name = name;
            /* if the 'from' file is dirty, we should sync it */
            if (bhh->flag & BH_DIRTY) {
                __bh_sync(bhh);
            }
            __put_bhhead(bhh);
        }
    }

    saved_ms = ms;
    saved_ms.name = strdup(saved_ms.name);
    isaved = 1;
    memset(&ms, 0, sizeof(ms));
    hvfs_info(lib, "saved_ms.name=%s\n", saved_ms.name);

    /* cleanup */
    xfree(dup);
    xfree(dup2);
    xfree(spath);

    /* do new create now */
    dup = strdup(to);
    dup2 = strdup(to);
    pino = g_msb.root_ino;

    path = dirname(dup);
    name = basename(dup2);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit2;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit2:
    if (name && strlen(name) > 0 && strcmp(name, "/") != 0) {
        /* final stat on target */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err == -ENOENT) {
            /* it is ok to continue */
        } else if (err) {
            hvfs_err(lib, "do internal stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        } else {
            /* target file or directory do exist */
            if (S_ISDIR(ms.mdu.mode)) {
                if (!S_ISDIR(saved_ms.mdu.mode)) {
                    err = -EISDIR;
                    goto out;
                }
                /* check if it is empty */
                if (__mmfs_is_empty_dir(ms.ino)) {
                    /* FIXME: delete the directory now, SAVED it to
                     * deleted_ms */
                    deleted_ms = ms;
                    err = __mmfs_unlink(pino, &ms, __MMFS_UNLINK_ALL);
                    if (err) {
                        hvfs_err(lib, "do internal unlink on _IN_%ld "
                                 "failed w/ %d\n",
                                 ms.ino, err);
                        goto out;
                    }
                } else {
                    err = -ENOTEMPTY;
                    goto out;
                }
            } else {
                if (S_ISDIR(saved_ms.mdu.mode)) {
                    err = -ENOTDIR;
                    goto out;
                }
                /* FIXME: delete the file now, check if it is SYMLINK */
                if (ms.mdu.flags & MMFS_MDU_SYMLINK) {
                    err = __mmfs_readlink(ms.pino, &ms);
                    if (err) {
                        hvfs_err(lib, "do internal stat(SYMLINK) on '%s' "
                                 "failed w/ %d\n",
                                 name, err);
                        err = -EINVAL;
                        goto out;
                    }
                }
                deleted_ms = ms;
                err = __mmfs_unlink(pino, &ms, __MMFS_UNLINK_ALL);
                if (err) {
                    hvfs_err(lib, "do internal unlink on _IN_%ld "
                             "failed w/ %d\n",
                             ms.ino, err);
                    goto out;
                }
            }
        }
    } else {
        /* this means the target is a directory and do exist */
        if (S_ISDIR(ms.mdu.mode)) {
            /* check if it is empty */
            if (__mmfs_is_empty_dir(pino)) {
                /* FIXME: delete the directory now, SAVED it to
                 * deleted_ms */
                deleted_ms = ms;
                err = __mmfs_unlink(pino, &ms, __MMFS_UNLINK_ALL);
                if (err) {
                    hvfs_err(lib, "do internal unlink on "
                             "_IN_%ld failed w/ %d\n",
                             pino, err);
                }
            } else {
                err = -ENOTEMPTY;
                goto out;
            }
        } else {
            hvfs_err(lib, "directory or not-directory, it is a question\n");
            goto out;
        }
    }
    hvfs_info(lib, "saved_ms.name=%s\n", saved_ms.name);

    ms.name = name;
    ms.ino = saved_ms.ino;
    ms.mdu = saved_ms.mdu;
    ms.arg = saved_ms.arg;

    {
        struct mdu_update mu = {.valid = 0,};
        u32 flags = __MMFS_CREATE_DENTRY;

        err = __mmfs_create(pino, &ms, &mu, flags);
        if (err) {
            hvfs_err(lib, "do internal create on '%s' failed w/ %d\n",
                     name, err);
            goto out_rollback;
        }
    }

    /* if the target file has been opened, we should update the ODC cached
     * info */
    {
        struct bhhead *bhh = __odc_lookup(ms.ino);

        if (bhh) {
            bhh->ms.pino = pino;
            __put_bhhead(bhh);
        }
    }

    hvfs_info(lib, "saved_ms.name=%s\n", saved_ms.name);
    /* unlink the old file or directory now (only dentry) */
    err = __mmfs_unlink(saved_ms.pino, &saved_ms, __MMFS_UNLINK_DENTRY);
    if (err) {
        hvfs_err(lib, "do internal unlink on (pino %ld)/%s failed "
                 "w/ %d (ignore)\n",
                 saved_ms.pino, saved_ms.name, err);
        /* ignore this error */
    }

    hvfs_debug(lib, "rename from %s(ino %ld) to %s(ino %ld)\n",
               from, saved_ms.ino, to, ms.ino);
out:
    if (isaved)
        xfree(saved_ms.name);
    xfree(sname);
    xfree(dup);
    xfree(dup2);
    xfree(spath);

    return err;
out_rollback:
    {
        /* rollback the unlink of target */
        struct mdu_update mu = {0,};
        u32 flags = __MMFS_CREATE_ALL;

        if (deleted_ms.mdu.flags & MMFS_MDU_SYMLINK)
            flags |= __MMFS_CREATE_SYMLINK;
        err = __mmfs_create(deleted_ms.pino, &deleted_ms, &mu, flags);
        if (err) {
            hvfs_err(lib, "do rollback create on (pino %ld)/%ld "
                     "failed w/ %d\n",
                     deleted_ms.pino, deleted_ms.ino, err);
        }
    }
    goto out;
}

static int mmfs_link(const char *from, const char *to)
{
    struct mstat ms = {0,}, saved_ms;
    char *dup = strdup(from), *dup2 = strdup(from),
        *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    /* Step 1: get the stat info of 'from' file */
    path = dirname(dup);
    name = basename(dup2);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    if (name && strlen(name) > 0 && strcmp(name, "/") != 0) {
        /* we have to lookup this file now. Otherwise what we want to lookup
         * is the last directory, just return a result string now */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        if (S_ISDIR(ms.mdu.mode)) {
            hvfs_err(lib, "hard link on directory is not allowed\n");
            err = -EPERM;
            goto out;
        }
        err = __mmfs_linkadd(&ms, 1);
        if (err) {
            hvfs_err(lib, "do hard link on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
    } else {
        hvfs_err(lib, "hard link on directory is not allowed\n");
        err = -EPERM;
        goto out;
    }

    saved_ms = ms;

    /* cleanup */
    xfree(dup);
    xfree(dup2);
    xfree(spath);

    /* Step 2: construct the new target entry */
    dup = strdup(to);
    dup2 = strdup(to);
    pino = g_msb.root_ino;

    path = dirname(dup);
    name = basename(dup2);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit2;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out_unlink;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit2:
    /* create the file in parent directory (only dentry) */
    if (strlen(name) == 0 || strcmp(name, "/") == 0) {
        hvfs_err(lib, "Create zero-length named file or root directory?\n");
        err = -EINVAL;
        goto out_unlink;
    }

    ms.name = name;
    ms.ino = saved_ms.ino;
    {
        struct mdu_update mu = {.valid = 0,};

        err = __mmfs_create(pino, &ms, &mu, __MMFS_CREATE_DENTRY);
        if (err) {
            hvfs_err(lib, "do internal create on '%s' failed w/ %d\n",
                     name, err);
            goto out_unlink;
        }
    }
out:
    xfree(dup);
    xfree(dup2);
    xfree(spath);

    return err;
out_unlink:
    {
        err = __mmfs_linkadd(&saved_ms, -1);
        if (err) {
            hvfs_err(lib, "do linkadd(-1) on '%s' failed w/ %d\n",
                     saved_ms.name, err);
        }
    }
    goto out;
}

static int mmfs_chmod(const char *pathname, mode_t mode)
{
    struct mstat ms = {0,};
    struct mdu_update mu = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    mu.valid = MU_MODE | MU_CTIME;
    mu.mode = mode;
    mu.ctime = time(NULL);

    /* finally, do update now */
    if (!name || strlen(name) == 0 || strcmp(name, "/") == 0) {
        /* update the final directory by ino */
        err = __mmfs_update_inode(&ms, &mu);
        if (err) {
            hvfs_err(lib, "do internal update on _IN_%ld failed w/ %d\n",
                     ms.ino, err);
            goto out;
        }
    } else {
        /* update the final file by name */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        __odc_update(&ms);
        err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err) {
            hvfs_err(lib, "do internal update on '%s'(_IN_%ld) "
                     "failed w/ %d\n",
                     name, ms.ino, err);
            goto out;
        }
        __odc_update(&ms);
    }
out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_chown(const char *pathname, uid_t uid, gid_t gid)
{
    struct mstat ms = {0,};
    struct mdu_update mu = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    mu.valid = MU_UID | MU_GID | MU_CTIME;
    mu.uid = uid;
    mu.gid = gid;
    mu.ctime = time(NULL);

    /* finally, do update now */
    if (!name || strlen(name) == 0 || strcmp(name, "/") == 0) {
        /* update the final directory by ino */
        err = __mmfs_update_inode(&ms, &mu);
        if (err) {
            hvfs_err(lib, "do internal update on _IN_%ld failed w/ %d\n",
                     ms.ino, err);
            goto out;
        }
    } else {
        /* update the final file by name */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        __odc_update(&ms);
        err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err) {
            hvfs_err(lib, "do internal update on '%s'(_IN_%ld) "
                     "failed w/ %d\n",
                     name, ms.ino, err);
            goto out;
        }
        __odc_update(&ms);
    }
out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_large_truncate(struct mstat *ms, off_t nsize)
{
    int err = 0;

    return err;
}

static int mmfs_truncate(const char *pathname, off_t size)
{
    struct mstat ms = {0,};
    struct mdu_update mu = {.valid = 0,};
    char *dup = strdup(pathname), *path, *name;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    ssize_t rlen;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    /* lookup the file in the parent directory now */
    if (name && strlen(name) > 0 && strcmp(name, "/") != 0) {
        /* we have to lookup this file now. Otherwise, what we want to lookup
         * is the last directory, just return a result string now */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
    } else {
        hvfs_err(lib, "truncate directory is not allowed\n");
        err = -EINVAL;
        goto out;
    }
    if (S_ISDIR(ms.mdu.mode)) {
        hvfs_err(lib, "truncate directory is not allowed\n");
        err = -EINVAL;
        goto out;
    }

    /* ok, we should check whether this file is a large or small file */
    if (ms.mdu.flags & MMFS_MDU_LARGE) {
        err = mmfs_large_truncate(&ms, size);
        goto out;
    }

    u64 oldsize = ms.mdu.size;

    /* check the file length now */
    if (size > ms.mdu.size) {
        void *data;

        data = xmalloc(size);
        if (!data) {
            hvfs_err(lib, "Expanding the file content w/ xmalloc failed\n");
            err = -ENOMEM;
            goto out;
        }

        rlen = __mmfs_fread(&ms, &data, 0, ms.mdu.size);
        if (rlen < 0) {
            hvfs_err(lib, "do internal fread on '%s' failed w/ %ld\n",
                     name, rlen);
            err = rlen;
            goto local_out;
        }
        memset(data + ms.mdu.size, 0, size - ms.mdu.size);

        err = __mmfs_fwrite(&ms, 0, data, size);
        if (err) {
            hvfs_err(lib, "do internal fwrite on '%s' failed w/ %d\n",
                     name, err);
            goto local_out;
        }
    local_out:
        xfree(data);
        if (err < 0)
            goto out;
    } else if (size == ms.mdu.size) {
        goto out;
    }

    /* finally update the metadata */
    mu.valid |= MU_SIZE | MU_MTIME;
    mu.size = size;
    mu.mtime = time(NULL);

    __odc_update(&ms);
    err = __mmfs_update_inode_proxy(&ms, &mu);
    if (err) {
        hvfs_err(lib, "do internal update on ino<%ld> failed w/ %d\n",
                 ms.ino, err);
        goto out;
    }
    __odc_update(&ms);

    __update_msb(MMFS_SB_U_SPACE, size - oldsize);
out:
    xfree(dup);

    return err;
}

static int mmfs_ftruncate(const char *pathname, off_t size,
                          struct fuse_file_info *fi)
{
    struct mstat ms = {0,};
    struct mdu_update mu = {.valid = 0,};
    struct bhhead *bhh = (struct bhhead *)fi->fh;
    ssize_t rlen;
    int err = 0;

    if (unlikely(!bhh))
        return -EBADF;

    ms = bhh->ms;

    /* ok, we should check whether this file is a large or small file */
    if (ms.mdu.flags & MMFS_MDU_LARGE) {
        err = mmfs_large_truncate(&ms, size);
        goto out;
    }

    /* check the file length now */
    if (size > ms.mdu.size) {
        void *data;

        data = xmalloc(size);
        if (!data) {
            hvfs_err(lib, "Expanding the file content w/ xmalloc failed\n");
            err = -ENOMEM;
            goto out;
        }

        rlen = __mmfs_fread(&ms, &data, 0, ms.mdu.size);
        if (rlen < 0) {
            if (rlen == -EFBIG) {
                /* translate EFBIG to OK */
                rlen = 0;
            } else {
                hvfs_err(lib, "do internal fread on '%s' failed w/ %ld\n",
                         pathname, rlen);
                err = rlen;
                goto local_out;
            }
        }
        if (rlen != ms.mdu.size) {
            hvfs_err(lib, "_IN_%ld fread size mismatch, expect %ld, got %ld\n",
                     ms.ino, ms.mdu.size, rlen);
            err = -EINVAL;
            goto local_out;
        }
        memset(data + ms.mdu.size, 0, size - ms.mdu.size);

        err = __mmfs_fwrite(&ms, 0, data, size);
        if (err) {
            hvfs_err(lib, "do internal fwrite on '%s' failed w/ %d\n",
                     pathname, err);
            goto local_out;
        }
    local_out:
        xfree(data);
        if (err < 0)
            goto out;
    } else if (size == ms.mdu.size) {
        goto out;
    }

    /* finally update the metadata */
    mu.valid |= MU_SIZE | MU_MTIME;
    mu.size = size;
    mu.mtime = time(NULL);

    __odc_update(&ms);
    err = __mmfs_update_inode_proxy(&ms, &mu);
    if (err) {
        hvfs_err(lib, "do internal update on ino<%ld> failed w/ %d\n",
                 ms.ino, err);
        goto out;
    }
    __odc_update(&ms);
    __update_msb(MMFS_SB_U_SPACE, mu.size - bhh->osize);
    bhh->osize = mu.size;

out:
    return err;
}

static int mmfs_utime(const char *pathname, struct utimbuf *buf)
{
    struct mstat ms = {0,};
    struct mdu_update mu = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    mu.valid = MU_ATIME | MU_MTIME;
    mu.atime = buf->actime;
    mu.mtime = buf->modtime;

    /* finally, do update now */
    if (!name || strlen(name) == 0 || strcmp(name, "/") == 0) {
        /* update the final directory by ino */
        err = __mmfs_update_inode(&ms, &mu);
        if (err) {
            hvfs_err(lib, "do internal update on _IN_%ld failed w/ %d\n",
                     ms.ino, err);
            goto out;
        }
    } else {
        /* update the final file by name */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        __odc_update(&ms);
        err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err) {
            hvfs_err(lib, "do internal update on '%s'(_IN_%ld) "
                     "failed w/ %d\n",
                     name, ms.ino, err);
            goto out;
        }
        __odc_update(&ms);
    }
out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_open(const char *pathname, struct fuse_file_info *fi)
{
    struct mstat ms = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    /* eh, we have to lookup this file now. Otherwise, what we want to lookup
     * is the last directory, just reutrn a result string now */
    ms.name = name;
    ms.ino = 0;
    err = __mmfs_stat(pino, &ms);
    if (err) {
        hvfs_err(lib, "do internal file stat on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }
    if (S_ISDIR(ms.mdu.mode)) {
        err = -EISDIR;
        goto out;
    }

    fi->fh = (u64)__get_bhhead(&ms);
    if (!fi->fh) {
        err = -EIO;
        goto out;
    }

    /* we should restat the file to detect any new file syncs */
#ifdef FUSE_SAFE_OPEN
    {
        struct bhhead *bhh = (struct bhhead *)fi->fh;

        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal file 2nd stat on '%s' "
                     "failed w/ %d\n",
                     name, err);
            goto out;
        }
        if (MDU_VERSION_COMPARE(ms.mdu.version, bhh->ms.mdu.version)) {
            bhh->ms.mdu = ms.mdu;
        }
        hvfs_warning(lib, "in open the file(%p, %ld)!\n",
                     bhh, bhh->ms.mdu.size);
    }
#endif

out:
    xfree(dup);
    xfree(spath);

    return err;
}

static int mmfs_reopen(const char *pathname, struct fuse_file_info *fi);

static inline
int mmfs_large_read(const char *pathname, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi)
{
    return 0;
}

static int mmfs_read(const char *pathname, char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
    struct mstat ms = {0,};
    struct bhhead *bhh = (struct bhhead *)fi->fh;
    ssize_t rlen;
    int err = 0, bytes = 0;

    if (bhh->ms.mdu.flags & MMFS_MDU_LARGE) {
        return mmfs_large_read(pathname, buf, size, offset, fi);
    }

    ms = bhh->ms;

    hvfs_debug(lib, "1. offset=%ld, size=%ld, bhh->size=%ld, bhh->asize=%ld\n",
               (u64)offset, (u64)size, bhh->size, bhh->asize);

    /* if the buffer is larger than file size, truncate it to size */
    if (offset + size > bhh->asize) {
        size = bhh->asize - offset;
    }
    /* if we can read ZERO length data, just return 0 */
    if ((ssize_t)size <= 0) {
        return 0;
    }
    hvfs_debug(lib, "2. offset=%ld, size=%ld, bhh->size=%ld, bhh->asize=%ld\n",
               (u64)offset, (u64)size, bhh->size, bhh->asize);

    err = __bh_read(bhh, buf, offset, size);
    if (err == -EFBIG) {
        /* read in the data now */
        /* NOTE: each fread will read-in the total file content, thus, do not
         * waste them, put them in the bh cache.
         */
        size_t rsize = size;
        off_t ooffset = offset;
        void **rbuf = (void **)&buf;
        void *nbuf = NULL;

        /* enlarge the read to chunk size or mdu.size */
        if (offset + size < ms.mdu.size) {
            rsize = ms.mdu.size;
            offset = 0;
            nbuf = xmalloc(rsize);
            if (!nbuf) {
                hvfs_err(lib, "xmalloc() %ld B failed, fallback to SLOW mode.\n",
                         (u64)rsize);
                rbuf = (void **)&buf;
                rsize = size;
                offset = ooffset;
            }
            rbuf = &nbuf;
        }
        
        rlen = __mmfs_fread(&ms, rbuf, offset, rsize);
        if (rlen < 0) {
            if (rlen == -EFBIG) {
                /* translate EFBIG to OK */
                err = 0;
            } else {
                hvfs_err(lib, "do internal fread on '%s' failed w/ %ld\n",
                         pathname, rlen);
                err = rlen;
            }
            goto out;
        }
        bytes = rlen;
        if (rbuf != (void **)&buf) {
            err = __bh_fill(&ms, bhh, *rbuf + ooffset, ooffset, size, 1);
            if (err < 0) {
                hvfs_err(lib, "fill the buffer cache [%ld,%ld] failed w/ %d\n",
                         (u64)ooffset, (u64)ooffset + size, err);
                xfree(*rbuf);
                goto out;
            }
            /* Note that, we fill the buffer as more as we can, but do NOT
             * update the block if it has already existed. */
            err = __bh_fill(&ms, bhh, *rbuf + offset, offset, rlen, 0);
            if (err < 0) {
                hvfs_err(lib, "fill the buffer cache [%ld,%ld] failed w/ %d\n",
                         (u64)offset, (u64)offset + rlen, err);
                xfree(*rbuf);
                goto out;
            }
            memcpy(buf, *rbuf + ooffset, size);
            xfree(*rbuf);
            bytes = size;
        } else {
            err = __bh_fill(&ms, bhh, buf, offset, rlen, 1);
            if (err < 0) {
                hvfs_err(lib, "fill the buffer cache failed w/ %d\n",
                         err);
                goto out;
            }
        }
        
        /* restore the bytes and some mstat fields */
        err = bytes;
        ms.pino = bhh->ms.pino;
    } else if (err < 0) {
        hvfs_err(lib, "buffer cache read '%s' failed w/ %d\n",
                 pathname, err);
        goto out;
    }

    if (!mmfs_fuse_mgr.noatime && err > 0) {
        /* update the atime now */
        struct mdu_update mu;
        struct timeval tv;
        int __err;

        gettimeofday(&tv, NULL);

        mu.valid = MU_ATIME;
        mu.atime = tv.tv_sec;
        __err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err < 0) {
            hvfs_err(lib, "do internal update on '%s' failed w/ %d\n",
                     pathname, __err);
            goto out;
        }
    }

out:
    return err;
}

static int mmfs_cached_write(const char *pathname, const char *buf,
                             size_t size, off_t offset,
                             struct fuse_file_info *fi)
{
    struct mstat ms;
    struct bhhead *bhh = (struct bhhead *)fi->fh;
    int err = 0;

    ms = bhh->ms;
    __set_bhh_dirty(bhh);
    if (offset + size > bhh->asize)
        bhh->asize = offset + size;

    err = __bh_fill(&ms, bhh, (void *)buf, offset, size, 1);
    if (err < 0) {
        hvfs_err(lib, "fill the buffer cache failed w/ %d\n",
                 err);
        goto out;
    }
    err = size;

out:
    return err;
}

static int mmfs_write(const char *pathname, const char *buf,
                      size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
    struct bhhead *bhh = (struct bhhead *)fi->fh;

    hvfs_debug(lib, "in write the file %s(%p, mdu.size=%ld) [%ld,%ld)!\n",
               pathname, bhh, bhh->ms.mdu.size,
               (u64)offset, (u64)offset + size);

    if (offset + size > MMFS_LARGE_FILE_CHUNK)
        bhh->ms.mdu.flags |= MMFS_MDU_LARGE;
    
    return mmfs_cached_write(pathname, buf, size, offset, fi);
}

static int mmfs_statfs_plus(const char *pathname, struct statvfs *stbuf)
{
    struct statfs s;
    int err = 0;

    memset(&s, 0, sizeof(s));

    /* construct the result buffer */
    stbuf->f_bsize = g_pagesize;
    stbuf->f_frsize = 0;
    stbuf->f_blocks = g_msb.space_quota / stbuf->f_bsize;
    stbuf->f_bfree = (g_msb.space_quota - g_msb.space_used) / stbuf->f_bsize;
    stbuf->f_bavail = stbuf->f_bfree;
    stbuf->f_files = g_msb.inode_quota;
    stbuf->f_ffree = g_msb.inode_quota - g_msb.inode_used;
    stbuf->f_fsid = 0xff8888f5;
    stbuf->f_flag = ST_NOSUID;
    stbuf->f_namemax = 256;

    return err;
}

static int mmfs_release(const char *pathname, struct fuse_file_info *fi)
{
    struct bhhead *bhh = (struct bhhead *)fi->fh;

    if (bhh->flag & BH_DIRTY) {
        __bh_sync(bhh);
    } else if (bhh->ms.mdu.flags & MMFS_MDU_LARGE) {
        if (bhh->asize != bhh->ms.mdu.size) {
            struct mdu_update mu;
            struct mstat ms = bhh->ms;
            int err;

            mu.valid = MU_SIZE;
            mu.size = bhh->asize;

            err = __mmfs_update_inode_proxy(&ms, &mu);
            if (err) {
                hvfs_err(lib, "do internal update on ino<%lx> "
                         "failed w/ %d\n",
                         ms.ino, err);
            }
            hvfs_warning(lib, "in release the file(%p,%ld,%ld)!\n",
                         bhh, bhh->asize, bhh->ms.mdu.size);
        }
    }
    
    __put_bhhead(bhh);

    mmfs_update_sb(&g_msb);

    return 0;
}

int mmfs_reopen(const char *pathname, struct fuse_file_info *fi)
{
    int err = 0;

    /* release the bhhead */
    mmfs_release(pathname, fi);

    /* reopen it! */
    err = mmfs_open(pathname, fi);
    if (err) {
        hvfs_err(lib, "mmfs_open(%s) failed w/ %d\n",
                 pathname, err);
        goto out;
    }

out:
    return err;
}

/* mmfs_fsync(): we sync the buffered data and write-back any metadata changes.
 *
 * Note: right now, we just ignore datasync flag
 */
static int mmfs_fsync(const char *pathname, int datasync,
                      struct fuse_file_info *fi)
{
    struct bhhead *bhh = (struct bhhead *)fi->fh;
    int err = 0;

    if (bhh->flag & BH_DIRTY) {
        __bh_sync(bhh);
    } else if (bhh->ms.mdu.flags & MMFS_MDU_LARGE) {
    }

    return err;
}

static int mmfs_opendir(const char *pathname, struct fuse_file_info *fi)
{
    struct mstat ms = {0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    mmfs_dir_t *dir;
    int err = 0;

    dir = xzalloc(sizeof(*dir));
    if (!dir) {
        hvfs_err(lib, "xzalloc() mmfs_dir_t failed\n");
        return -ENOMEM;
    }
    dir->cursor = "0";

    fi->fh = (u64)dir;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
hit:
    if (name && strlen(name) > 0 && strcmp(name, "/") != 0) {
        /* stat the last dir */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do last dir stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
    } else {
        /* check if it is the root directory */
        if (pino == g_msb.root_ino) {
            err = __mmfs_fill_root(&ms);
            if (err) {
                hvfs_err(lib, "fill root entry failed w/ %d\n", err);
                goto out;
            }
        }
    }

    dir->dino = ms.ino;
out:
    xfree(dup);
    xfree(spath);

    return err;
}

/* If we have read some dirents, we return 0; otherwise, we should return 1 to
 * indicate a error.
 */
static int __mmfs_readdir_plus(void *buf, fuse_fill_dir_t filler, 
                               off_t off, mmfs_dir_t *dir)
{
    char name[256];
    struct dentry_info *tdi;
    off_t saved_offset = off;
    int err = 0, res = 0;

    /* check if the cached entries can serve the request */
    if (off < dir->goffset) {
        /* seek backward, just zero out our brain */
        xfree(dir->di);
        dir->di = NULL;
        dir->cursor = "0";
    }
    hvfs_debug(lib, "readdir_plus ino %ld off %ld goff %ld csize %d\n",
               dir->dino, off, dir->goffset, dir->csize);

    if (dir->csize > 0 &&
        off <= dir->goffset + dir->csize) {
        /* ok, easy to fill the dentry */
        struct stat st;
        int idx;
            
        tdi = dir->di;
        for (idx = 0; idx < dir->csize; idx++) {
            if (dir->goffset + idx == off) {
                /* fill in */
                memcpy(name, tdi->name, tdi->namelen);
                name[tdi->namelen] = '\0';
                memset(&st, 0, sizeof(st));
                st.st_ino = tdi->ino;
                st.st_mode = tdi->mode;
                if (filler != NULL) {
                    res = filler(buf, name, &st, off + 1);
                } else {
                    hvfs_info(lib, "FILLER: buf %p name %s ino %ld "
                              "mode %d(%o) off %ld\n",
                              buf, name, st.st_ino, st.st_mode,
                              st.st_mode, off + 1);
                }
                if (res)
                    break;
                /* update offset */
                dir->loffset = idx + 1;
                off++;
            }
            tdi = (void *)tdi + sizeof(*tdi) + tdi->namelen;
        }
            
        if (res)
            return 0;
    }

    do {
        /* find by hscan cursor */
        dir->goffset += dir->csize;
        dir->loffset = 0;
        dir->csize = 0;
        xfree(dir->di);
        dir->di = NULL;
        res = 0;
        
        if (strcmp(dir->cursor, "0") == 0 && 
            dir->goffset + dir->csize > 0) {
            /* safely break now */
            break;
        }
    
        err = __mmfs_readdir(dir);
        if (err) {
            hvfs_err(lib, "__mmfs_readdir() failed w/ %d\n", err);
            goto out;
        }
        /* check if we should stop */
        if (off <= dir->goffset + dir->csize) {
            struct stat st;
            int idx;

            tdi = dir->di;
            for (idx = 0; idx < dir->csize; idx++) {
                if (dir->goffset + idx == off) {
                    /* fill in */
                    memcpy(name, tdi->name, tdi->namelen);
                    name[tdi->namelen] = '\0';
                    st.st_ino = tdi->ino;
                    st.st_mode = tdi->mode;
                    if (filler != NULL)
                        res = filler(buf, name, &st, off + 1);
                    else {
                        hvfs_debug(lib, "FILLER: buf %p name %s ino %ld "
                                   "mode %d(%o) off %ld\n",
                                   buf, name, st.st_ino, st.st_mode, 
                                   st.st_mode, off + 1);
                    }
                    if (res)
                        break;
                    dir->loffset = idx + 1;
                    off++;
                }
                tdi = (void *)tdi + sizeof(*tdi) + tdi->namelen;
            }
        }
        break;
    } while (1);
        
    if (off > saved_offset)
        err = 0;
    else
        err = 1;

out:
    return err;
}

static int mmfs_readdir_plus(const char *pathname, void *buf,
                             fuse_fill_dir_t filler, off_t off,
                             struct fuse_file_info *fi)
{
    int err = 0;

    err = __mmfs_readdir_plus(buf, filler, off,
                              (mmfs_dir_t *)fi->fh);
    if (err < 0) {
        hvfs_err(lib, "do internal readdir on '%s' failed w/ %d\n",
                 pathname, err);
        goto out;
    } else if (err == 1) {
        /* stop loudly */
        err = -ENOENT;
    }

out:
    return err;
}

static int mmfs_release_dir(const char *pathname, struct fuse_file_info *fi)
{
    mmfs_dir_t *dir = (mmfs_dir_t *)fi->fh;

    xfree(dir->di);
    xfree(dir);

    return 0;
}

/* Introduced in fuse version 2.5. Create and open a file, thus we drag mknod
 * and open it it!
 */
static int mmfs_create_plus(const char *pathname, mode_t mode,
                            struct fuse_file_info *fi)
{
    struct mstat ms = {0,};
    struct mdu_update mu = {.valid = 0,};
    char *dup = strdup(pathname), *path, *name, *spath = NULL;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags = 0;
    int err = 0;

    SPLIT_PATHNAME(dup, path, name);
    n = path;

    spath = strdup(path);
    err = __ltc_lookup(spath, &pino, &mdu_flags);
    if (err > 0) {
        goto hit;
    }

    /* parse the path and do __stat on each directory */
    do {
        p = strtok_r(n, "/", &s);
        if (!p) {
            /* end */
            break;
        }
        hvfs_debug(lib, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(lib, "do internal dir stat on '%s' failed w/ %d\n",
                     p, err);
            break;
        }
        pino = ms.ino;
    } while (!(n = NULL));

    if (unlikely(err)) {
        goto out;
    }

    __ltc_update(spath, (void *)pino, (void *)(u64)ms.mdu.flags);
    mdu_flags = ms.mdu.flags;
hit:
    /* create the file or dir in the parent directory now */
    ms.name = name;
    ms.ino = 0;
    mu.valid = MU_MODE | MU_CTIME | MU_ATIME | MU_MTIME;
    mu.mode = mode;
    mu.atime = mu.mtime = mu.ctime = time(NULL);

    if (mdu_flags & MMFS_MDU_LARGE) {
        /* do something */
    }

    err = __mmfs_create(pino, &ms, &mu, __MMFS_CREATE_ALL);
    if (err) {
        hvfs_err(lib, "do internal create on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }

    fi->fh = (u64)__get_bhhead(&ms);
    if (!fi->fh) {
        err = -EIO;
        goto out;
    }

    {
        struct bhhead *bhh = (struct bhhead *)fi->fh;
        hvfs_warning(lib, "in create the file _IN_%ld (size=%ld)!\n",
                     bhh->ms.ino, bhh->ms.mdu.size);
    }
    /* Save the mstat in SOC cache */
    {
        struct soc_entry *se = __se_alloc(pathname, &ms);

        __soc_insert(se);
    }

out:
    xfree(dup);
    xfree(spath);

    return err;
}

static void *mmfs_init(struct fuse_conn_info *conn)
{
    int err = 0;

    if (!g_pagesize)
        g_pagesize = getpagesize();

realloc:
    err = posix_memalign(&zero_page, g_pagesize, g_pagesize);
    if (err || !zero_page) {
        goto realloc;
    }
    if (mprotect(zero_page, g_pagesize, PROT_READ) < 0) {
        hvfs_err(lib, "mprotect ZERO page failed w/ %d\n", errno);
    }

    err = mmcc_init("STL://127.0.0.1:26379");
    if (err) {
        hvfs_err(lib, "MMCC init() failed w/ %d\n", err);
        HVFS_BUGON("MMCC init failed!");
    }

    /* load create script now */
    err = __mmfs_load_scripts();
    if (err) {
        hvfs_err(lib, "__mmfs_load_scripts() failed w/ %d\n",
                 err);
        HVFS_BUGON("Script load failed. FATAL ERROR!\n");
    }

    if (!mmfs_fuse_mgr.inited) {
        mmfs_fuse_mgr.inited = 1;
        mmfs_fuse_mgr.sync_write = 0;
        mmfs_fuse_mgr.noatime = 1;
        mmfs_fuse_mgr.nodiratime = 1;
    }
    
    if (__ltc_init(mmfs_fuse_mgr.ttl, 0)) {
        hvfs_err(lib, "LRU Translate Cache init failed. Cache DISABLED!\n");
    }

    if (__odc_init(0)) {
        hvfs_err(lib, "OpeneD Cache(ODC) init failed. FATAL ERROR!\n");
        HVFS_BUGON("ODC init failed!");
    }

    if (__soc_init(0)) {
        hvfs_err(lib, "Stat Oneshot Cache(SOC) init failed. FATAL ERROR!\n");
        HVFS_BUGON("SOC init failed!");
    }

    /* init superblock */
    xlock_init(&g_msb.lock);
    if (mmfs_fuse_mgr.namespace)
        g_msb.name = mmfs_fuse_mgr.namespace;
    err = __mmfs_get_sb(&g_msb);
    if (err) {
        if (err == -EINVAL && mmfs_fuse_mgr.ismkfs) {
            hvfs_err(lib, "File System '%s' not exist, ok to create it.\n",
                     g_msb.name);
        } else {
            hvfs_err(lib, "Get superblock for file system '%s' failed w/ %d\n",
                     g_msb.name, err);
            HVFS_BUGON("Get superblock failed!");
        }
    } else {
        if (mmfs_fuse_mgr.ismkfs) {
            hvfs_err(lib, "File System '%s' superblock has already existed.\n",
                g_msb.name);
            HVFS_BUGON("File System already exists!");
        } else {
            hvfs_info(lib, "File System '%s' SB={root_ino=%ld,version=%ld,"
                      "space(%ld,%ld),inode(%ld,%ld)}\n",
                      g_msb.name, g_msb.root_ino, g_msb.version,
                      g_msb.space_quota, g_msb.space_used,
                      g_msb.inode_quota, g_msb.inode_used);
        }
    }
    
    return NULL;
}

static void mmfs_destroy(void *arg)
{
    __ltc_destroy();
    __odc_destroy();
    __soc_destroy();

    mmfs_update_sb(&g_msb);

    mmcc_debug_mode(1);
    mmcc_fina();
    mmcc_debug_mode(0);
    
    hvfs_info(lib, "Exit the MMFS fuse client now.\n");
}

struct fuse_operations mmfs_ops = {
    .getattr = mmfs_getattr,
    .readlink = mmfs_readlink,
    .getdir = NULL,
    .mknod = mmfs_mknod,
    .mkdir = mmfs_mkdir,
    .unlink = mmfs_unlink,
    .rmdir = mmfs_rmdir,
    .symlink = mmfs_symlink,
    .rename = mmfs_rename,
    .link = mmfs_link,
    .chmod = mmfs_chmod,
    .chown = mmfs_chown,
    .truncate = mmfs_truncate,
    .utime = mmfs_utime,
    .open = mmfs_open,
    .read = mmfs_read,
    .write = mmfs_write,
    .statfs = mmfs_statfs_plus,
    .flush = NULL,
    .release = mmfs_release,
    .fsync = mmfs_fsync,
    .setxattr = NULL,
    .getxattr = NULL,
    .listxattr = NULL,
    .removexattr = NULL,
    .opendir = mmfs_opendir,
    .readdir = mmfs_readdir_plus,
    .releasedir = mmfs_release_dir,
    .init = mmfs_init,
    .destroy = mmfs_destroy,
    .create = mmfs_create_plus,
    .ftruncate = mmfs_ftruncate,
};
