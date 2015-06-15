/**
 * Copyright (c) 2015 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-06-15 14:09:33 macan>
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

#define RENEW_CI(op) do {                       \
        __mmfs_renew_ci(&g_ci, op);             \
    } while (0)

struct __mmfs_fuse_mgr mmfs_fuse_mgr = {.inited = 0,
                                        .namespace = "default",
};

struct mmfs_sb g_msb = {
    .name = "default",
    .root_ino = MMFS_ROOT_INO,
};

static u32 hvfs_mmfs_tracing_flags = HVFS_DEFAULT_LEVEL;

static struct __mmfs_client_info g_ci;

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
                        hvfs_err(mmfs, "Reget superblock failed w/ %d\n", err);
                        goto out;
                    }
                    msb->d.space_used = space_used;
                    msb->d.inode_used = inode_used;
                    need_retry = 0;
                    goto retry;
                }
            }
            hvfs_err(mmfs, "Update superblock failed w/ %d\n", err);
        }
        hvfs_debug(mmfs, "Write superblock: {IU=%ld, SU=%ld} done.\n",
                   msb->inode_used, msb->space_used);
        msb->flags &= ~MMFS_SB_DIRTY;
    }
out:
    xlock_unlock(&msb->lock);
}

void mmfs_debug_mode(int enable)
{
    switch (enable) {
    case 30:
        hvfs_mmfs_tracing_flags = 0xf0000000;
        hvfs_mmll_tracing_flags = 0xffffffff;
        break;
    case 31:
        hvfs_mmfs_tracing_flags = 0xf0000001;
        hvfs_mmll_tracing_flags = 0xffffffff;
        break;
    case 32:
        hvfs_mmfs_tracing_flags = 0xf0000004;
        hvfs_mmll_tracing_flags = 0xffffffff;
        break;
    case 33:
        hvfs_mmfs_tracing_flags = 0xffffffff;
        hvfs_mmll_tracing_flags = 0xffffffff;
        break;
    case 20:
        hvfs_mmfs_tracing_flags = 0xf0000000;
        hvfs_mmll_tracing_flags = 0xf0000004;
        break;
    case 21:
        hvfs_mmfs_tracing_flags = 0xf0000001;
        hvfs_mmll_tracing_flags = 0xf0000004;
        break;
    case 22:
        hvfs_mmfs_tracing_flags = 0xf0000004;
        hvfs_mmll_tracing_flags = 0xf0000004;
        break;
    case 23:
        hvfs_mmfs_tracing_flags = 0xffffffff;
        hvfs_mmll_tracing_flags = 0xf0000004;
        break;
    case 10:
        hvfs_mmfs_tracing_flags = 0xffffffff;
        hvfs_mmll_tracing_flags = 0xf0000001;
        break;
    case 11:
        hvfs_mmfs_tracing_flags = 0xf0000001;
        hvfs_mmll_tracing_flags = 0xf0000001;
        break;
    case 12:
        hvfs_mmfs_tracing_flags = 0xf0000004;
        hvfs_mmll_tracing_flags = 0xf0000001;
        break;
    case 13:
        hvfs_mmfs_tracing_flags = 0xffffffff;
        hvfs_mmll_tracing_flags = 0xf0000001;
        break;
    case 3:
        hvfs_mmfs_tracing_flags = 0xffffffff;
        hvfs_mmll_tracing_flags = 0xf0000000;
        break;
    case 2:
        hvfs_mmfs_tracing_flags = 0xf0000004;
        hvfs_mmll_tracing_flags = 0xf0000000;
        break;
    case 1:
        hvfs_mmfs_tracing_flags = 0xf0000001;
        hvfs_mmll_tracing_flags = 0xf0000000;
        break;
    case 0:
    default:
        hvfs_mmfs_tracing_flags = 0xf0000000;
        hvfs_mmll_tracing_flags = 0xf0000000;
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
        hvfs_err(mmfs, "Stat Oneshot Cache(SOC) hash table init failed\n");
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
        hvfs_err(mmfs, "xzalloc() soc_entry failed\n");
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
#define MMFS_D_CHKNR    1
    struct chunk **chunks;
    size_t size;                /* total buffer size */
    size_t asize;               /* actually size for release use */
    size_t osize;               /* old size for last update */
    struct mstat ms;
    xrwlock_t clock;
    u64 ino;                    /* who am i? */
    u64 chknr;                  /* allocated chunks */
    void *ptr;                  /* private pointer */

#define BHH_CLEAN       0x00
#define BHH_DIRTY       0x01
#define BHH_SYNCING     0x80
    u32 flag;
    atomic_t ref;
};

struct chunk
{
    struct list_head bh;
    size_t size;                /* total buffer size */
    size_t asize;               /* actually size for release use */

    u64 chkid;                  /* chunk id */
#define CHUNK_CLEAN     0x00
#define CHUNK_DIRTY     0x01
    u32 flag;

    xlock_t lock;
};

struct bh
{
    struct list_head list;
    off_t offset;               /* buffer offset */
    void *data;                 /* this is always a page */
#define BH_INIT         0x00
#define BH_UP2DATE      0x01
#define BH_DIRTY        0x02
    u32 flag;
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
        hvfs_err(mmfs, "OpeneD Cache(ODC) hash table init failed\n");
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
        if (ms->mdu.blknr > 0)
            bhh->chknr = ms->mdu.blknr;
        else
            bhh->chknr = MMFS_D_CHKNR;
        bhh->chunks = xzalloc(sizeof(struct chunk *) * bhh->chknr);
        if (unlikely(!bhh->chunks)) {
            xfree(bhh);
            return NULL;
        }
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

static inline void __set_bhh_syncing(struct bhhead *bhh)
{
retry:
    xrwlock_wlock(&bhh->clock);
    if (bhh->flag & BHH_SYNCING) {
        /* another sync is doing now, just wait */
        xrwlock_wunlock(&bhh->clock);
        pthread_yield();
        goto retry;
    } else {
        bhh->flag |= BHH_SYNCING;
    }
    xrwlock_wunlock(&bhh->clock);
}

static inline void __clr_bhh_syncing(struct bhhead *bhh)
{
    xrwlock_wlock(&bhh->clock);
    bhh->flag &= ~BHH_SYNCING;
    xrwlock_wunlock(&bhh->clock);
}

static inline void __set_bhh_dirty(struct bhhead *bhh)
{
    bhh->flag |= BHH_DIRTY;
}
static inline void __clr_bhh_dirty(struct bhhead *bhh)
{
    bhh->flag &= ~BHH_DIRTY;
}

static inline int __is_bh_dirty(struct bh *bh)
{
    return bh->flag & BH_DIRTY;
}

static inline void __set_bh_dirty(struct bh *bh)
{
    bh->flag |= BH_DIRTY;
}

static inline void __clr_bh_dirty(struct bh *bh)
{
    bh->flag &= ~BH_DIRTY;
}

static inline int __is_bh_up2date(struct bh *bh)
{
    return bh->flag & BH_UP2DATE;
}

static inline void __set_bh_up2date(struct bh *bh)
{
    bh->flag |= BH_UP2DATE;
}

static inline void __clr_bhh_up2date(struct bh *bh)
{
    bh->flag &= ~BH_UP2DATE;
}

static inline int __is_chunk_dirty(struct chunk *c)
{
    return c->flag & CHUNK_DIRTY;
}

static inline void __set_chunk_dirty(struct chunk *c)
{
    c->flag |= CHUNK_DIRTY;
}

static inline void __clr_chunk_dirty(struct chunk *c)
{
    c->flag &= ~CHUNK_DIRTY;
}

static struct chunk *__get_chunk(u64 chkid)
{
    struct chunk *c;

    c = xzalloc(sizeof(*c));
    if (!c) {
        return NULL;
    }
    INIT_LIST_HEAD(&c->bh);
    xlock_init(&c->lock);
    c->chkid = chkid;
    c->flag = CHUNK_CLEAN;

    return c;
}

static int __enlarge_chunk_table(struct bhhead *bhh, u64 chkid)
{
    void *t;
    int nr = bhh->chknr;

    while (nr <= chkid) {
        if (nr > 128 * 1024) {
            nr += 1024;
        } else {
            nr *= 2;
        }
    }
    t = xrealloc(bhh->chunks, sizeof(struct chunk *) * nr);
    if (!t) {
        hvfs_err(mmfs, "__enlarge_chunk_table() to NR %d failed.\n",
                 nr);
        return -ENOMEM;
    }
    memset(t + bhh->chknr * sizeof(struct chunk *), 0, 
           sizeof(struct chunk *) * (nr - bhh->chknr));
    bhh->chknr = nr;
    bhh->chunks = t;

    return 0;
}

static void __put_bh(struct bh *bh)
{
    if (bh->data && bh->data != zero_page)
        xfree(bh->data);

    xfree(bh);
}

static void __put_chunk(struct chunk *c)
{
    struct bh *bh, *n;

    list_for_each_entry_safe(bh, n, &c->bh, list) {
        list_del(&bh->list);
        if (__is_bh_dirty(bh)) {
            hvfs_err(mmfs, "FATAL dirty BH offset %ld in CHK %ld "
                     "(size %ld, asize %ld flag %d)\n",
                     (u64)bh->offset, 
                     c->chkid, c->size, c->asize, c->flag);
        }
        __put_bh(bh);
    }
    
    xfree(c);
}

static struct chunk *__lookup_chunk(struct bhhead *bhh, u64 chkid, int lock)
{
    struct chunk *c;
    int err = 0;
    
    if (chkid >= bhh->chknr) {
        err = __enlarge_chunk_table(bhh, chkid);
        if (err) {
            hvfs_err(mmfs, "enlarge chunk table failed w/ %d\n",
                     err);
            return NULL;
        }
    }
    c = bhh->chunks[chkid];
    if (!c) {
        c = __get_chunk(chkid);
        if (c) {
            struct chunk *tc = NULL;
            
            /* try to insert it to bhhead */
            xrwlock_wlock(&bhh->clock);
            tc = bhh->chunks[chkid];
            if (!tc) {
                bhh->chunks[chkid] = c;
            }
            xrwlock_wunlock(&bhh->clock);
            if (tc) {
                /* someone ahead me, free myself */
                __put_chunk(c);
                c = tc;
            }
        }
    }
    if (c && lock)
        xlock_lock(&c->lock);

    return c;
}

static void __unlock_chunk(struct chunk *c)
{
    xlock_unlock(&c->lock);
}

/* Return Value: <0 error; =0 alloced; >0 existed
 */
static int __prepare_bh(struct bh *bh, int alloc)
{
    if (!bh->data || bh->data == zero_page) {
        if (alloc) {
            bh->data = xzalloc(g_pagesize);
            if (!bh->data) {
                return -ENOMEM;
            }

            return 0;
        } else
            bh->data = zero_page;
    }

    return 1;
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
    if (__prepare_bh(bh, alloc) < 0) {
        xfree(bh);
        bh = NULL;
    }

    return bh;
}

static void __put_bhhead(struct bhhead *bhh)
{
    struct chunk *c;

    if (__odc_remove(bhh)) {
        int i;

        for (i = 0; i < bhh->chknr; i++) {
            c = bhh->chunks[i];
            if (c)
                __put_chunk(c);
        }
        xfree(bhh->chunks);
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
            __odc_lock(bhh);
            if (!(bhh->flag & BHH_DIRTY))
                bhh->asize = ms->mdu.size;
            __odc_unlock(bhh);
        } else {
            ms->mdu = bhh->ms.mdu;
            ms->mdu.size = bhh->asize;
        }
        __put_bhhead(bhh);
    }
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
            /* restat the mdu */
            struct mstat xms = {0,};

            xms.ino = ms->ino;
            err = __mmfs_stat(0, &xms);
            if (err) {
                goto out_put;
            }
            __odc_update(&xms);

            __odc_lock(bhh);
            ms->mdu = bhh->ms.mdu;
            err = __mmfs_update_inode(ms, mu);
            __odc_unlock(bhh);
        }
    out_put:
        __put_bhhead(bhh);
    } else {
        err = __mmfs_update_inode(ms, mu);
    }

    return err;
}

static void __set_chunk_size(struct bhhead *bhh, u64 chkid)
{
    struct chunk *c = bhh->chunks[chkid];
    u64 chk_begin = chkid * g_msb.chunk_size;

    c->asize = min(bhh->asize - chk_begin, g_msb.chunk_size);
}

/* Note that, offset should be in-chunk offset
 */
static int __bh_fill_chunk(u64 chkid, struct mstat *ms, struct bhhead *bhh,
                           void *buf, off_t offset, size_t size, int update)
{
    /* round down the offset */
    struct chunk *c;
    struct bh *bh;
    off_t off_end = PAGE_ROUNDUP((offset + size), g_pagesize);
    off_t loff = 0;
    ssize_t rlen;
    size_t _size = 0;
    int err = 0, alloced = 0;

    c = __lookup_chunk(bhh, chkid, 0);
    if (!c) {
        hvfs_err(mmfs, "__lookup_chunk(%ld) CHK=%ld failed.\n",
                 ms->ino, chkid);
        return -ENOMEM;
    }

    xrwlock_wlock(&bhh->clock);
    hvfs_debug(mmfs, "__bh_fill_chunk(%ld) CHK=%ld offset=%ld size=%ld "
               "c->size=%ld c->asize=%ld bhh->size=%ld bhh->asize=%ld bhh->chknr=%ld"
               " bhh %p chunk %p update=%d\n",
               ms->ino, chkid, (u64)offset, (u64)size,
               c->size, c->asize,
               bhh->size, bhh->asize, bhh->chknr, bhh, c, update);

    if (update == 2) {
        u64 asize = offset + size + chkid * g_msb.chunk_size;

        __set_chunk_dirty(c);
        __set_bhh_dirty(bhh);
        if (asize > bhh->asize) {
            bhh->asize = asize;
        }
    }
    __set_chunk_size(bhh, chkid);

    if (offset >= c->size) {
        while (c->size < off_end) {
            bh = __get_bh(c->size, 0);
            if (!bh) {
                err = -ENOMEM;
                goto out_unlock;
            }
            if (offset == c->size && size >= g_pagesize) {
                /* just copy the buffer, prepare true page */
                alloced = __prepare_bh(bh, 1);
                _size = min(size, bh->offset + g_pagesize - offset);
                if (buf && (alloced | update)) {
                    memcpy(bh->data + offset - bh->offset,
                           buf + loff, _size);
                    if (update == 2) __set_bh_dirty(bh);
                }
                size -= _size;
                loff += _size;
                offset = bh->offset + g_pagesize;
            } else {
                /* read in the page if the bh is in BH_INIT; otherwise just
                 * copy data */
                if (c->size <= ms->mdu.size) {
                    __prepare_bh(bh, 1);
                }
                if (!__is_bh_up2date(bh)) {
                    rlen = __mmfs_fread(ms, bh->data, 
                                        chkid * g_msb.chunk_size + c->size, 
                                        g_pagesize);
                    if (rlen == -EFBIG) {
                        /* it is ok, we just zero the page */
                        err = 0;
                    } else if (rlen < 0) {
                        hvfs_err(mmfs, "bh_fill() read the file range [%ld, %ld] "
                                 "failed w/ %ld\n",
                                 c->size, c->size + g_pagesize, rlen);
                        err = rlen;
                        goto out_unlock;
                    }
                    __set_bh_up2date(bh);
                }
                /* should we fill with buf? */
                if (size && offset < bh->offset + g_pagesize) {
                    __prepare_bh(bh, 1);
                    _size = min(size, bh->offset + g_pagesize - offset);
                    if (buf) {
                        memcpy(bh->data + offset - bh->offset,
                               buf + loff, _size);
                        if (update == 2) __set_bh_dirty(bh);
                    }
                    size -= _size;
                    loff += _size;
                    offset = bh->offset + g_pagesize;
                }
            }
            list_add_tail(&bh->list, &c->bh);
            c->size += g_pagesize;
        }
    } else {
        /* update the cached content */
        list_for_each_entry(bh, &c->bh, list) {
            if (offset >= bh->offset && offset < bh->offset + g_pagesize) {
                alloced = __prepare_bh(bh, 1);
                _size = min(size, bh->offset + g_pagesize - offset);
                if (buf && (alloced | update)) {
                    memcpy(bh->data + offset - bh->offset,
                           buf + loff, _size);
                    if (update == 2) __set_bh_dirty(bh);
                }
                size -= _size;
                loff += _size;
                offset = bh->offset + g_pagesize;
                if (size <= 0)
                    break;
            }
        }
        if (size) {
            /* fill the last holes */
            while (c->size < off_end) {
                bh = __get_bh(c->size, 1);
                if (!bh) {
                    err = -ENOMEM;
                    goto out_unlock;
                }
                if (offset == c->size && size >= g_pagesize) {
                    /* just copy the buffer */
                    _size = min(size, bh->offset + g_pagesize - offset);
                    if (buf) {
                        memcpy(bh->data + offset - bh->offset,
                               buf + loff, _size);
                        if (update == 2) __set_bh_dirty(bh);
                    }
                    size -= _size;
                    loff += _size;
                    offset = bh->offset + g_pagesize;
                } else {
                    /* read in the page if the bh is clean; otherwise just
                     * copy the data */
                    if (!__is_bh_up2date(bh)) {
                        rlen = __mmfs_fread(ms, bh->data, 
                                            chkid * g_msb.chunk_size + c->size, 
                                            g_pagesize);
                        if (rlen == -EFBIG) {
                            /* it is ok, we just zero the page */
                            err = 0;
                        } else if (rlen < 0) {
                            hvfs_err(mmfs, "bh_fill() read the file range [%ld, %ld] "
                                     "failed w/ %ld",
                                     c->size, c->size + g_pagesize, rlen);
                            err = rlen;
                            goto out_unlock;
                        }
                        __set_bh_up2date(bh);
                    }
                    /* should we fill with buf? */
                    if (size && offset < bh->offset + g_pagesize) {
                        _size = min(size, bh->offset + g_pagesize - offset);
                        if (buf) {
                            memcpy(bh->data + offset - bh->offset,
                                   buf + loff, _size);
                            if (update == 2) __set_bh_dirty(bh);
                        }
                        size -= _size;
                        loff += _size;
                        offset = bh->offset + g_pagesize;
                    }
                }
                list_add_tail(&bh->list, &c->bh);
                c->size += g_pagesize;
            }
        }
    }

out_unlock:
    xrwlock_wunlock(&bhh->clock);

    return err;
}

/* __bh_fill() will fill the buffer cache w/ buf. if there are holes, it will
 * fill them automatically with in a chunk.
 */
static int __bh_fill(struct mstat *ms, struct bhhead *bhh, 
                     void *buf, off_t offset, size_t size, int update)
{
    u64 chkid, endchk;
    s64 loff, lsize, end = offset + size, bytes = 0;
    int err = 0;

    chkid = offset / g_msb.chunk_size;
    endchk = (offset + size) / g_msb.chunk_size;
    endchk -= (offset + size) % g_msb.chunk_size == 0 ? 1 : 0;
    
    hvfs_debug(mmfs, "_bh_fill(%ld) offset=%ld size=%ld bhh->size=%ld "
               "bhh->asize=%ld in CHK=[%ld,%ld]\n",
               ms->ino, (u64)offset, (u64)size, bhh->size, bhh->asize,
               chkid, endchk);

    for (; chkid <= endchk; chkid++) {
        loff = offset - chkid * g_msb.chunk_size;
        if (loff < 0) loff = 0;
        lsize = min(g_msb.chunk_size - loff, 
                    end - chkid * g_msb.chunk_size - loff);

        err = __bh_fill_chunk(chkid, ms, bhh,
                              buf + bytes, loff, lsize, update);
        if (err) {
            hvfs_err(mmfs, "_IN_%ld fill chunk %ld @ [%ld,%ld) faild w/ %d\n",
                     ms->ino, chkid, loff, lsize, err);
            goto out;
        }
        bhh->size = max(bhh->size, (chkid + 1) * g_msb.chunk_size);
        bytes += lsize;
    }

out:
    return err;
}

/* Return the cached bytes we can read or minus errno
 *
 * Note that: offset should be in-chunk offset.
 */
static int __bh_read_chunk(struct bhhead *bhh, void *buf, off_t offset, 
                           size_t size, u64 chkid)
{
    struct chunk *c;
    struct bh *bh;
    off_t loff = 0, saved_offset = offset;
    size_t _size, saved_size = size;

    c = __lookup_chunk(bhh, chkid, 1);
    if (!c) {
        hvfs_err(mmfs, "__lookup_chunk(%ld) CHK=%ld failed.\n",
                 bhh->ms.ino, chkid);
        return -ENOMEM;
    }

    if (offset + size > c->size || list_empty(&c->bh)) {
        __unlock_chunk(c);
        return -EFBIG;
    }

    hvfs_debug(mmfs, "__bh_read_chunk() for _IN_%ld [%ld,%ld) CHK=%ld, "
               "c->size=%ld c->asize=%ld\n",
               bhh->ms.ino, offset, offset + size, 
               chkid, c->size, c->asize);
    
    xrwlock_rlock(&bhh->clock);
    list_for_each_entry(bh, &c->bh, list) {
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
    if (saved_offset + size > c->asize) {
        size = c->asize - saved_offset;
        if ((ssize_t)size < 0)
            size = 0;
    }
    __unlock_chunk(c);

    return size;
}

static int __bh_read(struct bhhead *bhh, void *buf, off_t offset,
                     size_t size)
{
    u64 chkid, endchk, lastchk;
    s64 loff, lsize, end = offset + size;
    int bytes = 0, rlen;
    int err, j;

    chkid = offset / g_msb.chunk_size;
    endchk = (offset + size) / g_msb.chunk_size;
    endchk -= (offset + size) % g_msb.chunk_size == 0 ? 1 : 0;
    lastchk = bhh->asize / g_msb.chunk_size;
    lastchk -= bhh->asize % g_msb.chunk_size == 0 ? 1 : 0;

    hvfs_debug(mmfs, "__bh_read(%ld) [%ld,%ld) in CHK[%ld,%ld]\n",
               bhh->ms.ino, offset, offset + size, chkid, endchk);

    for (j = 0; chkid <= endchk; chkid++, j++) {
        loff = offset - chkid * g_msb.chunk_size;
        if (loff < 0) loff = 0;
        lsize = min(g_msb.chunk_size - loff, 
                    end - chkid * g_msb.chunk_size - loff);

        err = __bh_read_chunk(bhh, buf + bytes, loff, lsize, chkid);
        hvfs_debug(mmfs, "__bh_read_chunk(%ld) CHK=%ld loff=%ld "
                   "lsize=%ld return %d\n", 
                   bhh->ms.ino, chkid, loff, lsize, err);
        if (err == -EFBIG) {
            /* try to read this WHOLE chunk from file(only one request can do
             * it */
            void *cdata;

            cdata = xmalloc(g_msb.chunk_size);
            if (!cdata) {
                /* This SHOULD BE TEST: FIXME */
                hvfs_warning(mmfs, "xmalloc() chunk buffer failed, slow mode.\n");

                rlen = __mmfs_fread(&bhh->ms, buf + bytes, offset + bytes, lsize);
                if (rlen < 0) {
                    if (rlen == EHOLE && chkid < lastchk) {
                        rlen = -EFBIG;
                    }
                    if (rlen == -EFBIG) {
                        /* translate EFBIG to OK */
                        err = 0;
                        rlen = 0;
                    } else {
                        hvfs_err(mmfs, "do internal fread on _IN_%ld failed w/ %d\n",
                                 bhh->ms.ino, rlen);
                        err = rlen;
                        goto out;
                    }
                }
                /* ok, fill the buffer cache */
                if (rlen > 0) {
                    err = __bh_fill(&bhh->ms, bhh, buf + bytes, 
                                    offset + bytes, lsize, 1);
                    if (err < 0) {
                        hvfs_err(mmfs, "fill the buffer cache [%ld,%ld) failed w/ %d\n",
                                 (u64)offset + bytes, 
                                 (u64)offset + bytes + lsize, err);
                        goto out;
                    }
                }
                if (rlen < lsize) {
                    /* partial read: if it is the last chunk, break now;
                     * otherwise, zero the remain buffer */
                    hvfs_warning(mmfs, "partial chunk read, expect %ld, get %d\n",
                                 lsize, rlen);
                    bytes += rlen;
                    if (chkid < lastchk) {
                        memset(buf + bytes, 0, lsize - rlen);
                        bytes += lsize - rlen;
                    }
                } else if (rlen > lsize) {
                    hvfs_err(mmfs, "chunk read beyond range, expect %ld, get %d\n",
                             lsize, rlen);
                    bytes += lsize;
                } else {
                    bytes += rlen;
                }
            } else {
                struct chunk *c = __lookup_chunk(bhh, chkid, 1);

                if (c->size > 0) {
                    xfree(cdata);
                    __unlock_chunk(c);
                    return -EAGAIN;
                }
                rlen = __mmfs_fread(&bhh->ms, cdata, chkid * g_msb.chunk_size,
                                    g_msb.chunk_size);
                if (rlen < 0) {
                    if (rlen == EHOLE && chkid < lastchk) {
                        rlen = -EFBIG;
                    }
                    if (rlen == -EFBIG) {
                        /* translate EFBIG to OK, zero this chunk? */
                        err = 0;
                        rlen = 0;
                    } else {
                        hvfs_err(mmfs, "do internal fread on _IN_%ld failed w/ %d\n",
                                 bhh->ms.ino, rlen);
                        err = rlen;
                        xfree(cdata);
                        __unlock_chunk(c);
                        goto out;
                    }
                }
                /* ok, fill the buffer cache */
                if (rlen > 0) {
                    err = __bh_fill(&bhh->ms, bhh, cdata, chkid * g_msb.chunk_size,
                                    rlen, 1);
                    if (err < 0) {
                        hvfs_err(mmfs, "fill the buffer cache [%ld,%ld) failed w/ %d\n",
                                 (chkid) * g_msb.chunk_size, 
                                 (chkid + 1) * g_msb.chunk_size,
                                 err);
                        xfree(cdata);
                        __unlock_chunk(c);
                        goto out;
                    }
                }
                __unlock_chunk(c);
                if (rlen >= lsize) {
                    memcpy(buf + bytes, cdata + loff, lsize);
                    bytes += lsize;
                } else {
                    /* partial read: if it is the last chunk, break now;
                     * otherwise, zero the remain buffer
                     */
                    memcpy(buf + bytes, cdata + loff, rlen);
                    bytes += rlen;
                    if (chkid < lastchk) {
                        memset(buf + bytes, 0, lsize - rlen);
                        bytes += lsize - rlen;
                    }
                }
                xfree(cdata);
            }
        } else if (err < 0) {
            hvfs_err(mmfs, "buffer cache read _IN_%ld failed w/ %d\n",
                     bhh->ms.ino, err);
            goto out;
        } else {
            /* check for partial chunk read */
            bytes += err;
            if (chkid < lastchk && err < lsize) {
                memset(buf + bytes, 0, lsize - err);
                bytes += lsize - err;
            }
        }
    }
    err = bytes;

out:
    return err;
}

static int __bh_sync_chunk(struct bhhead *bhh, struct chunk *c, u64 chkid)
{
    struct mstat ms;
    struct bh *bh;
    struct iovec *iov = NULL;
    off_t offset = 0;
    void *data = NULL;
    size_t size, _size;
    int err = 0, i = 0;

    ms = bhh->ms;
    
    c = __lookup_chunk(bhh, chkid, 1);
    if (!c) {
        hvfs_err(mmfs, "__lookup_chunk(%ld) CHK=%ld failed.\n",
                 bhh->ms.ino, chkid);
        return -ENOMEM;
    }

    hvfs_debug(mmfs, "__bh_sync_chunk(%ld) CHK=%ld c->size=%ld "
               "c->asize=%ld\n",
               ms.ino, chkid, c->size, c->asize);

    xrwlock_wlock(&bhh->clock);
    if (!__is_chunk_dirty(c)) {
        goto out_unlock;
    }

    size = c->asize;
    list_for_each_entry(bh, &c->bh, list) {
        _size = min(size, g_pagesize);
        i++;
        size -= _size;
        if (size <= 0)
            break;
    }

    if (i > IOV_MAX - 5) {
        /* sadly fallback to memcpy approach */
        data = xmalloc(c->asize);
        if (!data) {
            hvfs_err(mmfs, "xmalloc(%ld) data buffer failed\n", 
                     c->asize);
            xrwlock_wunlock(&bhh->clock);
            __unlock_chunk(c);
            return -ENOMEM;
        }

        size = c->asize;
        list_for_each_entry(bh, &c->bh, list) {
            _size = min(size, g_pagesize);
            memcpy(data + offset, bh->data, _size);
            __clr_bh_dirty(bh);
            offset += _size;
            size -= _size;
            if (size <= 0)
                break;
        }
    } else {
        iov = xmalloc(sizeof(*iov) * i);
        if (!iov) {
            hvfs_err(mmfs, "xmalloc() iov buffer failed\n");
            xrwlock_wunlock(&bhh->clock);
            __unlock_chunk(c);
            return -ENOMEM;
        }
        
        size = c->asize;
        i = 0;
        list_for_each_entry(bh, &c->bh, list) {
            _size = min(size, g_pagesize);
            
            __clr_bh_dirty(bh);
            (iov + i)->iov_base = bh->data;
            (iov + i)->iov_len = _size;
            i++;
            size -= _size;
            if (size <= 0)
                break;
        }
    }
    __clr_chunk_dirty(c);
out_unlock:
    xrwlock_wunlock(&bhh->clock);
    __unlock_chunk(c);

    /* write out the data now */
    if (data) {
        err = __mmfs_fwrite(&ms, 0, data, c->asize, chkid);
        if (err) {
            hvfs_err(mmfs, "do internal fwrite on ino'%lx' failed w/ %d\n",
                     ms.ino, err);
            goto out_free;
        }
    } else {
        err = __mmfs_fwritev(&ms, 0, iov, i, chkid);
        if (err) {
            hvfs_err(mmfs, "do internal fwrite on ino'%lx' failed w/ %d\n",
                     ms.ino, err);
            goto out_free;
        }
    }

out_free:
    xfree(data);
    xfree(iov);

    return err;
}

static int __bh_sync_(struct bhhead *bhh, u32 valid)
{
    struct mstat ms;
    int err = 0, i;

    __clr_bhh_dirty(bhh);

    /* set bhh syncing, and wait for other syncs if needed */
    __set_bhh_syncing(bhh);

    ms = bhh->ms;

    if (bhh->asize > bhh->size) {
        /* oh, we have to fill the remain pages */
        err = __bh_fill(&ms, bhh, NULL, bhh->asize, 0, 1);
        if (err < 0) {
            hvfs_err(mmfs, "fill the buffer cache failed w/ %d\n",
                     err);
            goto out;
        }
        ms.pino = bhh->ms.pino;
    }

    hvfs_debug(mmfs, "__bh_sync(%ld) size=%ld asize %ld mdu.size %ld\n",
               ms.ino, bhh->size, bhh->asize, bhh->ms.mdu.size);

    /* sync for each dirty chunk */
    for (i = 0; i < bhh->chknr; i++) {
        struct chunk *c = bhh->chunks[i];

        if (c && c->flag & CHUNK_DIRTY) {
            err = __bh_sync_chunk(bhh, c, i);
            if (err < 0) {
                hvfs_err(mmfs, "__bh_sync_chunk(%d) failed w/ %d\n",
                         i, err);
            }
        }
    }

    /* update the file attributes */
    {
        struct mdu_update mu = {0,};

        mu.valid = MU_SIZE | MU_MTIME | MU_BLKNR;
        mu.size = bhh->asize;
        mu.mtime = time(NULL);
        mu.blknr = mu.size / g_msb.chunk_size + 1;
        mu.blknr -= mu.size % g_msb.chunk_size == 0 ? 1 : 0;
        if (valid & MU_CTIME) {
            mu.ctime = time(NULL);
            mu.valid |= MU_CTIME;
        }

        __odc_update(&ms);
        err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err) {
            hvfs_err(mmfs, "do internal update on ino<%lx> failed w/ %d\n",
                     ms.ino, err);
            goto out;
        }

        __odc_update(&ms);
        __update_msb(MMFS_SB_U_SPACE, mu.size - bhh->osize);
        bhh->osize = mu.size;
        /* finally, update bhh->hs */
        bhh->ms = ms;
    }

out:
    __clr_bhh_syncing(bhh);

    return err;
}

static inline int __bh_sync(struct bhhead *bhh)
{
    return __bh_sync_(bhh, 0);
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
    g_mmfs_tick = &g_client_tick;

    if (hsize)
        mmfs_ltc_mgr.hsize = hsize;
    else
        mmfs_ltc_mgr.hsize = MMFS_LTC_HSIZE_DEFAULT;

    mmfs_ltc_mgr.ttl = ttl;

    mmfs_ltc_mgr.ht = xmalloc(mmfs_ltc_mgr.hsize * sizeof(struct regular_hash));
    if (!mmfs_ltc_mgr.ht) {
        hvfs_err(mmfs, "LRU Translate Cache hash table init failed\n");
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
    struct regular_hash *rh;
    struct ltc_entry *le;
    struct hlist_node *pos, *n;
    int i;
    
    /* need to free every LTC entry */
    for (i = 0; i < mmfs_ltc_mgr.hsize; i++) {
        rh = mmfs_ltc_mgr.ht + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry_safe(le, pos, n, &rh->h, hlist) {
            hlist_del(&le->hlist);
            xfree(le->fullname);
            xfree(le);
        }
        xlock_unlock(&rh->lock);
    }
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
            xfree(se->key);
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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
            hvfs_debug(mmfs, "do internal file stat on '%s'"
                       " failed w/ %d pino %ld (RT %ld)\n",
                       name, err, pino, g_msb.root_ino);
            goto out;
        }
    } else {
        /* check if it is the root directory */
        if (pino == g_msb.root_ino) {
            /* stat root w/o any file name, it is ROOT we want to stat */
            err = __mmfs_fill_root(&ms);
            if (err) {
                hvfs_err(mmfs, "fill root entry failed w/ %d\n", err);
                goto out;
            }
        }
    }

    /* update ms w/ local ODC cached mstat */
    {
        struct bhhead *bhh = __odc_lookup(ms.ino);

        if (unlikely(bhh)) {
            hvfs_debug(mmfs, "1. ODC update size? v%d,%d, bhh->asize=%ld, mdu.size=%ld\n",
                       ms.mdu.version, bhh->ms.mdu.version, bhh->asize, ms.mdu.size);
            if (MDU_VERSION_COMPARE(ms.mdu.version, bhh->ms.mdu.version)) {
                /* FIXME: this means that server's mdu has been updated. We
                 * should clean up the bh cache here! */
                bhh->ms.mdu = ms.mdu;
                __odc_lock(bhh);
                if (!(bhh->flag & BHH_DIRTY))
                    bhh->asize = ms.mdu.size;
                __odc_unlock(bhh);
            } else {
                ms.mdu = bhh->ms.mdu;
                ms.mdu.size = bhh->asize;
            }
            __put_bhhead(bhh);
            hvfs_debug(mmfs, "2. ODC update size? v%d,%d, bhh->asize=%ld, mdu.size=%ld\n",
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

    RENEW_CI(OP_GETATTR);
    
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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
    } else {
        hvfs_err(mmfs, "Readlink from a directory is not allowed.\n");
        err = -EINVAL;
        goto out;
    }

    /* ok to parse the symname */
    {
        err = __mmfs_readlink(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "readlink on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        memset(buf, 0, size);
        memcpy(buf, ms.arg, min(ms.mdu.size, size));
        xfree(ms.arg);
    }

out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_READLINK);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
    mu.mode = mode | S_IFBLK;
    mu.dev = rdev;
    err = __mmfs_create(pino, &ms, &mu, __MMFS_CREATE_ALL);
    if (err) {
        hvfs_err(mmfs, "do internal create on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }

out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_MKNOD);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        hvfs_err(mmfs, "do internal create on '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do internal stat on _IN_%ld failed w/ %d\n",
                     pino, err);
            goto out;
        }
    }
    err = __mmfs_update_inode(&pms, &mu);
    if (err) {
        hvfs_err(mmfs, "do internal update on _IN_%ld failed w/ %d\n",
                 pms.ino, err);
        goto out;
    }

out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_MKDIR);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        hvfs_err(mmfs, "do internal delete on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }
    if (!S_ISLNK(ms.mdu.mode))
        __update_msb(MMFS_SB_U_SPACE, -ms.mdu.size);

out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_UNLINK);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        hvfs_err(mmfs, "Reject root directory removal!\n");
        err = -ENOTEMPTY;
        goto out;
    } else {
        /* confirm what it is firstly! */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        if (!S_ISDIR(ms.mdu.mode)) {
            hvfs_err(mmfs, "not a directory, we expect dir here.\n");
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
            hvfs_err(mmfs, "do internal delete on '%s' failed w/ %d\n",
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
                hvfs_err(mmfs, "do internal stat on _IN_%ld failed w/ %d\n",
                         pino, err);
                goto out;
            }
        }
        err = __mmfs_update_inode(&pms, &mu);
        if (err) {
            hvfs_err(mmfs, "do internal update on _IN_%ld failed w/ %d\n",
                     pms.ino, err);
            goto out;
        }

        /* delete the MMServer space */
        if (!__mmfs_is_shadow_dir(ms.ino)) {
            char set[256];

            sprintf(set, "o%ld", ms.ino);
            err = mmcc_del_set(set);
            if (err) {
                hvfs_err(mmfs, "do MMCC set %s delete failed, manual delete.\n",
                         set);
                goto out;
            }
            hvfs_debug(mmfs, "MMCC set %s deleted (not shadow).\n", set);
        }
    }
out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_RMDIR);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        hvfs_err(mmfs, "Create zero-length named file or root directory?\n");
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
        hvfs_err(mmfs, "do internal create on '%s' failed w/ %d\n",
                 name, err);
        goto out;
    }
out:
    xfree(dup);
    xfree(spath);
    
    RENEW_CI(OP_SYMLINK);

    return err;
}

/* Rational for (atomic) rename:
 *
 * Basically, we stat and copy the file info to target location; and finally,
 * unlink the original entry.
 *
 */
static int mmfs_rename(const char *from, const char *to)
{
    struct mstat ms, saved_ms, deleted_ms = {.ino = 0, .mdu.mode = 0,};
    char *dup = strdup(from), *dup2 = strdup(from),
        *path, *name, *spath = NULL, *sname;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
    u32 mdu_flags;
    int err = 0, isaved = 0, deleted_file = 0;

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do internal stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        if (ms.mdu.flags & MMFS_MDU_SYMLINK) {
            err = __mmfs_readlink(ms.pino, &ms);
            if (err) {
                hvfs_err(mmfs, "do internal stat(SYMLINK) on '%s' "
                         "failed w/ %d\n",
                         name, err);
                goto out;
            }
        }
    } else {
        /* rename directory, it is ok */
        if (!S_ISDIR(ms.mdu.mode) || ms.ino == g_msb.root_ino) {
            hvfs_err(mmfs, "directory or not-directory, it is a question!\n");
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
            if (bhh->flag & BHH_DIRTY) {
                __bh_sync(bhh);
            }
            __put_bhhead(bhh);
        }
    }

    saved_ms = ms;
    saved_ms.name = strdup(saved_ms.name);
    isaved = 1;
    memset(&ms, 0, sizeof(ms));

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do internal stat on '%s' failed w/ %d\n",
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
                        hvfs_err(mmfs, "do internal unlink on _IN_%ld "
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
                        hvfs_err(mmfs, "do internal stat(SYMLINK) on '%s' "
                                 "failed w/ %d\n",
                                 name, err);
                        err = -EINVAL;
                        goto out;
                    }
                }
                deleted_ms = ms;
                err = __mmfs_unlink(pino, &ms, __MMFS_UNLINK_ALL);
                if (err) {
                    hvfs_err(mmfs, "do internal unlink on _IN_%ld "
                             "failed w/ %d\n",
                             ms.ino, err);
                    goto out;
                }
                /* BUG-XXX: xfstest-generic-309: should update mtime and ctime
                 * of target directory mtime and ctime */
                deleted_file = 1;
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
                    hvfs_err(mmfs, "do internal unlink on "
                             "_IN_%ld failed w/ %d\n",
                             pino, err);
                }
            } else {
                err = -ENOTEMPTY;
                goto out;
            }
        } else {
            hvfs_err(mmfs, "directory or not-directory, it is a question\n");
            goto out;
        }
    }

    ms.name = name;
    ms.ino = saved_ms.ino;
    ms.mdu = saved_ms.mdu;
    ms.arg = saved_ms.arg;

    {
        struct mdu_update mu = {.valid = 0,};
        u32 flags = __MMFS_CREATE_DENTRY;

        err = __mmfs_create(pino, &ms, &mu, flags);
        if (err) {
            hvfs_err(mmfs, "do internal create on '%s' failed w/ %d\n",
                     name, err);
            goto out_rollback;
        }
    }

    /* mtime/ctime fix */
    if (deleted_file) {
        struct mstat xms = {0,};
        struct mdu_update xmu = {.valid = 0,};

        xmu.valid = MU_MTIME | MU_CTIME;
        xmu.mtime = xmu.ctime = time(NULL);

        xms.ino = pino;
        err = __mmfs_stat(pino, &xms);
        if (err) {
            hvfs_err(mmfs, "do internal file stat on target parent _IN_%ld "
                     "failed w/ %d, missing mtime/ctime update on this DIR.\n",
                     xms.ino, err);
        } else {
            err = __mmfs_update_inode_proxy(&xms, &xmu);
            if (err) {
                hvfs_err(mmfs, "do internal update on _IN_%ld failed w/ %d "
                         ", missing mtime/ctime update on this DIR.\n",
                         xms.ino, err);
            }
        }
    }

    /* check if file's parent directory changes */
    if (S_ISREG(saved_ms.mdu.mode) && pino != saved_ms.pino) {
        __mmfs_rename_log(saved_ms.ino, saved_ms.pino, pino);
        __mmfs_inc_shadow_dir(saved_ms.pino);
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

    /* unlink the old file or directory now (only dentry) */
    err = __mmfs_unlink(saved_ms.pino, &saved_ms, __MMFS_UNLINK_DENTRY);
    if (err) {
        hvfs_err(mmfs, "do internal unlink on (pino %ld)/%s failed "
                 "w/ %d (ignore)\n",
                 saved_ms.pino, saved_ms.name, err);
        /* ignore this error */
    }

    /* invalid the ltc entry if source is a directory */
    if (S_ISDIR(saved_ms.mdu.mode)) {
        __ltc_invalid(from);
    }

    /* nlink fix */
    if (S_ISDIR(saved_ms.mdu.mode)) {
        struct mstat __ms;

        /* src parent dir nlink-- */
        __ms.ino = saved_ms.pino;
        err = __mmfs_stat(0, &__ms);
        if (err) {
            hvfs_err(mmfs, "__mmfs_stat(%ld) failed w/ %d, nlink-- failed\n",
                     __ms.ino, err);
        } else {
            err = __mmfs_linkadd(&__ms, -1);
            if (err) {
                hvfs_err(mmfs, "__mmfs_linkadd(%ld) failed w/ %d, nlink-- failed\n",
                         __ms.ino, err);
            }
        }
        /* dst parent dir nlink++ */
        __ms.ino = pino;
        err = __mmfs_stat(0, &__ms);
        if (err) {
            hvfs_err(mmfs, "__mmfs_stat(%ld) failed w/ %d, nlink++ failed\n",
                     __ms.ino, err);
        } else {
            err = __mmfs_linkadd(&__ms, 1);
            if (err) {
                hvfs_err(mmfs, "__mmfs_linkadd(%ld) failed w/ %d, nlink++ failed\n",
                         __ms.ino, err);
            }
        }
        if (err) {
            hvfs_err(mmfs, "rename success but nlink fix failed, ignore\n");
            err = 0;
        }
    }

    hvfs_debug(mmfs, "rename from %s(ino %ld) to %s(ino %ld)\n",
               from, saved_ms.ino, to, ms.ino);
out:
    if (isaved)
        xfree(saved_ms.name);
    xfree(sname);
    xfree(dup);
    xfree(dup2);
    xfree(spath);

    RENEW_CI(OP_RENAME);

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
            hvfs_err(mmfs, "do rollback create on (pino %ld)/%ld "
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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do internal stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        if (S_ISDIR(ms.mdu.mode)) {
            hvfs_err(mmfs, "hard link on directory is not allowed\n");
            err = -EPERM;
            goto out;
        }
        err = __mmfs_linkadd(&ms, 1);
        if (err) {
            hvfs_err(mmfs, "do hard link on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
    } else {
        hvfs_err(mmfs, "hard link on directory is not allowed\n");
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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        hvfs_err(mmfs, "Create zero-length named file or root directory?\n");
        err = -EINVAL;
        goto out_unlink;
    }

    ms.name = name;
    ms.ino = saved_ms.ino;
    {
        struct mdu_update mu = {.valid = 0,};

        err = __mmfs_create(pino, &ms, &mu, __MMFS_CREATE_DENTRY);
        if (err) {
            hvfs_err(mmfs, "do internal create on '%s' failed w/ %d\n",
                     name, err);
            goto out_unlink;
        }
    }
out:
    xfree(dup);
    xfree(dup2);
    xfree(spath);

    RENEW_CI(OP_LINK);

    return err;
out_unlink:
    {
        err = __mmfs_linkadd(&saved_ms, -1);
        if (err) {
            hvfs_err(mmfs, "do linkadd(-1) on '%s' failed w/ %d\n",
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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        if (pino == g_msb.root_ino) {
            err = __mmfs_fill_root(&ms);
            if (err) {
                hvfs_err(mmfs, "fill root entry failed w/ %d\n", err);
                goto out;
            }
        }
        err = __mmfs_update_inode(&ms, &mu);
        if (err) {
            hvfs_err(mmfs, "do internal update on _IN_%ld failed w/ %d\n",
                     ms.ino, err);
            goto out;
        }
    } else {
        /* update the final file by name */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        __odc_update(&ms);
        err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err) {
            hvfs_err(mmfs, "do internal update on '%s'(_IN_%ld) "
                     "failed w/ %d\n",
                     name, ms.ino, err);
            goto out;
        }
        __odc_update(&ms);
    }
out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_CHMOD);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        if (pino == g_msb.root_ino) {
            err = __mmfs_fill_root(&ms);
            if (err) {
                hvfs_err(mmfs, "fill root entry failed w/ %d\n", err);
                goto out;
            }
        }
        err = __mmfs_update_inode(&ms, &mu);
        if (err) {
            hvfs_err(mmfs, "do internal update on _IN_%ld failed w/ %d\n",
                     ms.ino, err);
            goto out;
        }
    } else {
        /* update the final file by name */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        __odc_update(&ms);
        err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err) {
            hvfs_err(mmfs, "do internal update on '%s'(_IN_%ld) "
                     "failed w/ %d\n",
                     name, ms.ino, err);
            goto out;
        }
        __odc_update(&ms);
    }
out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_CHOWN);

    return err;
}

static int mmfs_truncate(const char *pathname, off_t size)
{
    struct mstat ms = {0,};
    char *dup = strdup(pathname), *path, *name;
    char *p = NULL, *n, *s = NULL;
    u64 pino = g_msb.root_ino;
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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
    } else {
        hvfs_err(mmfs, "truncate directory is not allowed\n");
        err = -EINVAL;
        goto out;
    }
    if (S_ISDIR(ms.mdu.mode)) {
        hvfs_err(mmfs, "truncate directory is not allowed\n");
        err = -EINVAL;
        goto out;
    }

    struct bhhead *bhh = __get_bhhead(&ms);
    u64 osize = bhh->asize;

    if (!bhh) {
        err = -EIO;
        goto out;
    }

    /* check the file length now */
    if (size == bhh->asize) {
        goto out_put;
    } else if (size > bhh->asize) {
        __set_bhh_dirty(bhh);
        bhh->asize = size;
        err = __bh_fill(&ms, bhh, NULL, bhh->asize, (size - bhh->asize), 2);
        if (err < 0) {
            hvfs_err(mmfs, "fill the buffer cache failed w/ %d\n", err);
            bhh->asize = osize;
            goto out_put;
        }
    } else {
        __set_bhh_dirty(bhh);
        bhh->asize = size;
    }

    /* finally update the metadata */
    if (bhh->flag & BHH_DIRTY)
        __bh_sync_(bhh, MU_CTIME);

out_put:
    __put_bhhead(bhh);
out:
    xfree(dup);

    RENEW_CI(OP_TRUNCATE);

    return err;
}

static int mmfs_ftruncate(const char *pathname, off_t size,
                          struct fuse_file_info *fi)
{
    struct mstat ms = {0,};
    struct bhhead *bhh = (struct bhhead *)fi->fh;
    u64 osize;
    int err = 0;

    if (unlikely(!bhh))
        return -EBADF;

    ms = bhh->ms;
    osize = bhh->asize;

    /* check the file length now */
    if (size == bhh->asize) {
        goto out;
    } else if (size > bhh->asize) {
        __set_bhh_dirty(bhh);
        bhh->asize = size;
        err = __bh_fill(&ms, bhh, NULL, bhh->asize, (size - bhh->asize), 0);
        if (err < 0) {
            hvfs_err(mmfs, "fill the buffer cache failed w/ %d\n", err);
            bhh->asize = osize;
            goto out;
        }
    } else {
        __set_bhh_dirty(bhh);
        bhh->asize = size;
    }

    /* finally update the metadata */
    if (bhh->flag & BHH_DIRTY)
        __bh_sync_(bhh, MU_CTIME);

out:
    RENEW_CI(OP_FTRUNCATE);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        if (pino == g_msb.root_ino) {
            err = __mmfs_fill_root(&ms);
            if (err) {
                hvfs_err(mmfs, "fill root entry failed w/ %d\n", err);
                goto out;
            }
        }
        err = __mmfs_update_inode(&ms, &mu);
        if (err) {
            hvfs_err(mmfs, "do internal update on _IN_%ld failed w/ %d\n",
                     ms.ino, err);
            goto out;
        }
    } else {
        /* update the final file by name */
        ms.name = name;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal file stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
        __odc_update(&ms);
        err = __mmfs_update_inode_proxy(&ms, &mu);
        if (err) {
            hvfs_err(mmfs, "do internal update on '%s'(_IN_%ld) "
                     "failed w/ %d\n",
                     name, ms.ino, err);
            goto out;
        }
        __odc_update(&ms);
    }
out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_UTIME);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
        hvfs_err(mmfs, "do internal file stat on '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do internal file 2nd stat on '%s' "
                     "failed w/ %d\n",
                     name, err);
            goto out;
        }
        if (MDU_VERSION_COMPARE(ms.mdu.version, bhh->ms.mdu.version)) {
            bhh->ms.mdu = ms.mdu;
        }
        hvfs_warning(mmfs, "in open the file(%p, %ld)!\n",
                     bhh, bhh->ms.mdu.size);
    }
#endif

out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_OPEN);

    return err;
}

static int mmfs_read(const char *pathname, char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
    struct mstat ms = {0,};
    struct bhhead *bhh = (struct bhhead *)fi->fh;
    int err = 0;

    ms = bhh->ms;

    hvfs_debug(mmfs, "[%ld] 1. offset=%ld, size=%ld, bhh->size=%ld, bhh->asize=%ld\n",
               ms.ino, (u64)offset, (u64)size, bhh->size, bhh->asize);

    /* if the buffer is larger than file size, truncate it to size */
    if (offset + size > bhh->asize) {
        size = bhh->asize - offset;
    }
    /* if we can read ZERO length data, just return 0 */
    if ((ssize_t)size <= 0) {
        return 0;
    }
    hvfs_debug(mmfs, "[%ld] 2. offset=%ld, size=%ld, bhh->size=%ld, bhh->asize=%ld\n",
               ms.ino, (u64)offset, (u64)size, bhh->size, bhh->asize);

retry:
    err = __bh_read(bhh, buf, offset, size);
    if (err < 0) {
        if (err == -EAGAIN)
            goto retry;
        hvfs_err(mmfs, "buffer cache read '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do internal update on '%s' failed w/ %d\n",
                     pathname, __err);
            goto out;
        }
    }

out:
    RENEW_CI(OP_READ);

    return err;
}

static int mmfs_cached_write(const char *pathname, const char *buf,
                             size_t size, off_t offset,
                             struct fuse_file_info *fi)
{
    struct mstat ms;
    struct bhhead *bhh = (struct bhhead *)fi->fh;
    u64 osize;
    int err = 0;

    ms = bhh->ms;
    __odc_lock(bhh);
    __set_bhh_dirty(bhh);
    osize = bhh->asize;
    if (offset + size > bhh->asize) {
        bhh->asize = offset + size;
    }
    __odc_unlock(bhh);

    err = __bh_fill(&ms, bhh, (void *)buf, offset, size, 2);
    if (err < 0) {
        hvfs_err(mmfs, "fill the buffer cache failed w/ %d\n",
                 err);
        bhh->asize = osize;
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
    int err = 0;

    hvfs_debug(mmfs, "in write the file %s(%p, ino=%ld, mdu.size=%ld) [%ld,%ld)!\n",
               pathname, bhh, bhh->ms.ino, bhh->ms.mdu.size,
               (u64)offset, (u64)offset + size);

    if (offset + size > g_msb.chunk_size)
        bhh->ms.mdu.flags |= MMFS_MDU_LARGE;
    
    err = mmfs_cached_write(pathname, buf, size, offset, fi);

    RENEW_CI(OP_WRITE);

    return err;
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

    RENEW_CI(OP_STATFS_PLUS);

    return err;
}

static int mmfs_release(const char *pathname, struct fuse_file_info *fi)
{
    struct bhhead *bhh = (struct bhhead *)fi->fh;

    if (bhh->flag & BHH_DIRTY) {
        __bh_sync(bhh);
    }
    
    __put_bhhead(bhh);

    mmfs_update_sb(&g_msb);

    RENEW_CI(OP_RELEASE);

    return 0;
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

    if (bhh->flag & BHH_DIRTY) {
        __bh_sync(bhh);
    } else if (bhh->ms.mdu.flags & MMFS_MDU_LARGE) {
    }

    RENEW_CI(OP_FSYNC);

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
        hvfs_err(mmfs, "xzalloc() mmfs_dir_t failed\n");
        return -ENOMEM;
    }

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
            hvfs_err(mmfs, "do last dir stat on '%s' failed w/ %d\n",
                     name, err);
            goto out;
        }
    } else {
        /* check if it is the root directory */
        if (pino == g_msb.root_ino) {
            err = __mmfs_fill_root(&ms);
            if (err) {
                hvfs_err(mmfs, "fill root entry failed w/ %d\n", err);
                goto out;
            }
        }
    }

    dir->dino = ms.ino;
out:
    xfree(dup);
    xfree(spath);

    RENEW_CI(OP_OPENDIR);

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
        u64 ino = dir->dino;
        memset(dir, 0, sizeof(*dir));
        dir->dino = ino;
    }
    hvfs_debug(mmfs, "readdir_plus ino %ld off %ld goff %ld csize %d\n",
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
                    hvfs_info(mmfs, "FILLER: buf %p name %s ino %ld "
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
        
        if (dir->cursor && strcmp(dir->cursor, "0") == 0 && 
            dir->goffset + dir->csize > 0) {
            /* safely break now */
            break;
        }
    
        err = __mmfs_readdir(dir);
        if (err) {
            hvfs_err(mmfs, "__mmfs_readdir() failed w/ %d\n", err);
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
                        hvfs_debug(mmfs, "FILLER: buf %p name %s ino %ld "
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
        hvfs_err(mmfs, "do internal readdir on '%s' failed w/ %d\n",
                 pathname, err);
        goto out;
    } else if (err == 1) {
        /* stop loudly */
        err = -ENOENT;
    }

out:
    RENEW_CI(OP_READDIR_PLUS);

    return err;
}

static int mmfs_release_dir(const char *pathname, struct fuse_file_info *fi)
{
    mmfs_dir_t *dir = (mmfs_dir_t *)fi->fh;

    xfree(dir->cursor);
    xfree(dir->di);
    xfree(dir);

    RENEW_CI(OP_RELEASE_DIR);

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
        hvfs_debug(mmfs, "token: %s\n", p);
        /* Step 1: find inode info by call __mmfs_stat */
        ms.name = p;
        ms.ino = 0;
        err = __mmfs_stat(pino, &ms);
        if (err) {
            hvfs_err(mmfs, "do internal dir stat on '%s' failed w/ %d\n",
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
    mu.mode = mode | S_IFREG;
    mu.atime = mu.mtime = mu.ctime = time(NULL);

    err = __mmfs_create(pino, &ms, &mu, __MMFS_CREATE_ALL);
    if (err) {
        hvfs_err(mmfs, "do internal create on '%s' failed w/ %d\n",
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
        hvfs_warning(mmfs, "in create the file _IN_%ld (size=%ld)!\n",
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

    RENEW_CI(OP_CREATE_PLUS);

    return err;
}

static void *mmfs_timer_main(void *arg)
{
    static time_t last = -1;
    time_t cur = *(time_t *)arg;
    int err = 0;

    if (last < 0)
        last = cur;

    if (cur - last >= 30) {
        err = __mmfs_client_info(&g_ci);
        if (err) {
            hvfs_err(mmfs, "Update client info failed w/ %d\n", err);
        }
        last = cur;
    }

    return NULL;
}

static void *mmfs_init(struct fuse_conn_info *conn)
{
    mmcc_config_t mc = {
        .tcb = mmfs_timer_main,
        .ti = 10,
        .rcc = 300,
    };
    int err = 0;

    if (!g_pagesize)
        g_pagesize = getpagesize();

    err = __mmfs_renew_ci(&g_ci, OP_NONE);
    if (err) {
        hvfs_err(mmfs, "Init client info failed w/ %d, ignore it\n",
                 err);
    }

realloc:
    err = posix_memalign(&zero_page, g_pagesize, g_pagesize);
    if (err || !zero_page) {
        goto realloc;
    }
    if (mprotect(zero_page, g_pagesize, PROT_READ) < 0) {
        hvfs_err(mmfs, "mprotect ZERO page failed w/ %d\n", errno);
    }

    err = mmcc_config(&mc);
    if (err) {
        hvfs_err(mmfs, "MMCC config() failed w/ %d\n", err);
        HVFS_BUGON("MMCC config failed!");
    }

    if (mmfs_fuse_mgr.uris)
        err = mmcc_init(mmfs_fuse_mgr.uris);
    else
        err = mmcc_init("STL://127.0.0.1:26379");
    if (err) {
        hvfs_err(mmfs, "MMCC init() failed w/ %d\n", err);
        HVFS_BUGON("MMCC init failed!");
    }

    /* load create script now */
    err = __mmfs_load_scripts();
    if (err) {
        hvfs_err(mmfs, "__mmfs_load_scripts() failed w/ %d\n",
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
        hvfs_err(mmfs, "LRU Translate Cache init failed. Cache DISABLED!\n");
    }

    if (__odc_init(0)) {
        hvfs_err(mmfs, "OpeneD Cache(ODC) init failed. FATAL ERROR!\n");
        HVFS_BUGON("ODC init failed!");
    }

    if (__soc_init(0)) {
        hvfs_err(mmfs, "Stat Oneshot Cache(SOC) init failed. FATAL ERROR!\n");
        HVFS_BUGON("SOC init failed!");
    }

    /* init superblock */
    xlock_init(&g_msb.lock);
    if (mmfs_fuse_mgr.namespace)
        g_msb.name = mmfs_fuse_mgr.namespace;
    err = __mmfs_get_sb(&g_msb);
    if (err) {
        if (err == -EINVAL && mmfs_fuse_mgr.ismkfs) {
            hvfs_err(mmfs, "File System '%s' not exist, ok to create it.\n",
                     g_msb.name);
        } else {
            hvfs_err(mmfs, "Get superblock for file system '%s' failed w/ %d\n",
                     g_msb.name, err);
            HVFS_BUGON("Get superblock failed!");
        }
    } else {
        if (mmfs_fuse_mgr.ismkfs) {
            hvfs_err(mmfs, "File System '%s' superblock has already existed.\n",
                g_msb.name);
            HVFS_BUGON("File System already exists!");
        } else {
            hvfs_info(mmfs, "File System '%s' SB={root_ino=%ld,version=%ld,"
                      "space(%ld,%ld),inode(%ld,%ld)}\n",
                      g_msb.name, g_msb.root_ino, g_msb.version,
                      g_msb.space_quota, g_msb.space_used,
                      g_msb.inode_quota, g_msb.inode_used);
        }
    }
    /* set chunk size */
    g_msb.chunk_size = MMFS_LARGE_FILE_CHUNK;
    
    return NULL;
}

static void mmfs_destroy(void *arg)
{
    __ltc_destroy();
    __odc_destroy();
    __soc_destroy();

    mmfs_update_sb(&g_msb);

    /* free any other resources */
    mprotect(zero_page, g_pagesize, PROT_WRITE);
    xfree(zero_page);
    __mmfs_unload_scripts();

    mmcc_debug_mode(1);
    mmcc_fina();
    mmcc_debug_mode(0);

    /* free g_ci resources (must after mmcc_fina) */
    xfree(g_ci.hostname);
    xfree(g_ci.ip);
    xfree(g_ci.md5);

    hvfs_info(mmfs, "Exit the MMFS fuse client now.\n");
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