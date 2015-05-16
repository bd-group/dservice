/**
 * Copyright (c) 2015 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2015-05-16 12:19:02 macan>
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

struct __mmfs_r_op
{
    char *opname;
    char *script;
    char *sha;
};

#define __MMFS_R_OP_CREATE_INODE        0
#define __MMFS_R_OP_CREATE_DENTRY       1
#define __MMFS_R_OP_DELETE_DENTRY       2
#define __MMFS_R_OP_UPDATE_INODE        3
#define __MMFS_R_OP_UPDATE_SB           4

struct __mmfs_r_op g_ops[] = {
    {
        "create_inode",
        "local t = redis.call('incr', 'INO_CUR'); if t then local t0 = t; t = t..\",\"..ARGV[2]; local v = redis.call('hget', KEYS[1]..t0, ARGV[3]); if v == false then v = '0' end; if v == ARGV[4] then redis.call('hset', KEYS[1]..t0, ARGV[1], t); return t; else return ARGV[2] end; else return ARGV[2] end",
    },
    {
        "create_dentry",
        "local t = redis.call('hexists', KEYS[1], ARGV[1]); if t == 1 then return 1; else redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); return 0; end"
    },
    {
        "delete_dentry",
        "local t = redis.call('hexists', KEYS[1], ARGV[1]); if t == 0 then return 0; else t = redis.call('hget', KEYS[1], ARGV[1]); redis.call('hdel', KEYS[1], ARGV[1]); return t; end",
    },
    {
        "update_inode",
        "local x = redis.call('exists', KEYS[1]); if x == 0 then return -1 end; local v = redis.call('hget', KEYS[1], ARGV[3]); if v == false then v = '0' end; if v == ARGV[4] then redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); if not (ARGV[6] == 'nil') then redis.call('hset', KEYS[1], ARGV[5], ARGV[6]) end; redis.call('hset', KEYS[1], ARGV[3], v+1); return 1; else return 0 end",
    },
    {
        "update_sb",
        "local x = redis.call('exists', KEYS[1]); if x == 0 then return nil end; local v = redis.call('hget', KEYS[1], 'version'); if v == false then v = '0' end; local v1 = redis.call('hget', KEYS[1], 'space_quota'); local v2 = redis.call('hget', KEYS[1], 'space_used'); local v3 = redis.call('hget', KEYS[1], 'inode_quota'); local v4 = redis.call('hget', KEYS[1], 'inode_used'); if v == ARGV[1] then redis.call('hmset', KEYS[1], 'root_ino',ARGV[2], 'space_quota', v1+ARGV[3], 'space_used', v2+ARGV[4], 'inode_quota', v3+ARGV[5], 'inode_used', v4+ARGV[6], 'version', v+1); return redis.call('hgetall', KEYS[1]); else return nil end"
    },
};

int __mmfs_load_scripts() 
{
    redisReply *rpy = NULL;
    int err = 0, i;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    for (i = 0; i < sizeof(g_ops) / sizeof(struct __mmfs_r_op); i++) {
        rpy = redisCommand(rc->rc, "script load %s", g_ops[i].script);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = -EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "script %d load failed w/ \n%s.\n", i, rpy->str);
            err = -EINVAL;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_info(lib, "Script %d %s \tloaded as '%s'.\n",
                      i, g_ops[i].opname, rpy->str);
            g_ops[i].sha = strdup(rpy->str);
        }
    out_free:
        freeReplyObject(rpy);
    }
out:
    putRC(rc);

    return err;
}

int __mmfs_fill_root(struct mstat *ms)
{
    int err = 0;

    memset(ms, 0, sizeof(*ms));
    ms->ino = g_msb.root_ino;
    err = __mmfs_stat(ms->ino, ms);
    if (err) {
        hvfs_err(lib, "do internal ROOT stat failed w/ %d\n",
                 err);
    }

    return err;
}

int __str2mdu(char *mstr, struct mdu *mdu)
{
    char *p = NULL, *n, *s = NULL;
    char *dup = strdup(mstr);
    int err = 0, i = 0;

    n = dup;
    do {
        p = strtok_r(n, ",", &s);
        if (!p) {
            break;
        }
        hvfs_verbose(lib, "token: %s\n", p);
        switch (i) {
        case 0:
            mdu->ino = (u64)atol(p);
            break;
        case 1:
            mdu->flags = (u64)atol(p);
            break;
        case 2:
            mdu->uid = (u32)atoi(p);
            break;
        case 3:
            mdu->gid = (u32)atoi(p);
            break;
        case 4:
            mdu->dev = (u32)atoi(p);
            break;
        case 5:
            mdu->mode = (u16)atoi(p);
            break;
        case 6:
            mdu->nlink = (u16)atoi(p);
            break;
        case 7:
            mdu->size = (u64)atol(p);
            break;
        case 8:
            mdu->atime = (u64)atol(p);
            break;
        case 9:
            mdu->ctime = (u64)atol(p);
            break;
        case 10:
            mdu->mtime = (u64)atol(p);
            break;
        case 11:
            mdu->blknr = (u64)atol(p);
            break;
        case 12:
            mdu->version = (u32)atoi(p);
            break;
        }
        i++;
    } while (!(n = NULL));

    xfree(dup);

    if (i < 13) {
        err = -EINVAL;
    }

    return err;
}

void __mdu2str(struct mdu *mdu, char *ostr, int ignore_ino)
{
    char *p = ostr;
    
    if (ostr == NULL)
        return;

    if (!ignore_ino)
        p += sprintf(p, "%ld,", mdu->ino);
    p += sprintf(p, "%ld,", mdu->flags);
    p += sprintf(p, "%d,", mdu->uid);
    p += sprintf(p, "%d,", mdu->gid);
    p += sprintf(p, "%d,", mdu->dev);
    p += sprintf(p, "%d,", mdu->mode);
    p += sprintf(p, "%d,", mdu->nlink);
    p += sprintf(p, "%ld,", mdu->size);
    p += sprintf(p, "%ld,", mdu->atime);
    p += sprintf(p, "%ld,", mdu->ctime);
    p += sprintf(p, "%ld,", mdu->mtime);
    p += sprintf(p, "%ld,", mdu->blknr);
    p += sprintf(p, "%d", mdu->version);
}

void __init_mdu(struct mdu *mdu, int is_dir)
{
    memset(mdu, 0, sizeof(*mdu));
    mdu->nlink = 1;
    if (is_dir) {
        mdu->nlink = 2;
    }
}

void __pack_mdu(struct mdu *mdu, struct mdu_update *mu)
{
    if (mu->valid & MU_MODE)
        mdu->mode = mu->mode;
    if (mu->valid & MU_UID)
        mdu->uid = mu->uid;
    if (mu->valid & MU_GID)
        mdu->gid = mu->gid;
    if (mu->valid & MU_FLAG_ADD)
        mdu->flags |= mu->flags;
    if (mu->valid & MU_ATIME)
        mdu->atime = mu->atime;
    if (mu->valid & MU_MTIME)
        mdu->mtime = mu->mtime;
    if (mu->valid & MU_CTIME)
        mdu->ctime = mu->ctime;
    if (mu->valid & MU_VERSION)
        mdu->version = mu->version;
    if (mu->valid & MU_SIZE)
        mdu->size = mu->size;
    if (mu->valid & MU_FLAG_CLR)
        mdu->flags &= ~(mu->flags);
    if (mu->valid & MU_NLINK)
        mdu->nlink = mu->nlink;
    if (mu->valid & MU_NLINK_DELTA)
        mdu->nlink += mu->nlink;
    if (mu->valid & MU_DEV)
        mdu->dev = mu->dev;
    if (mu->valid & MU_BLKNR)
        mdu->blknr = mu->blknr;
}

void __update_msb(int flag, s64 delta)
{
    if (delta == 0) return;

    hvfs_debug(lib, "Update MSB:{0x%x -> %ld}\n", flag, delta);
    xlock_lock(&g_msb.lock);
    switch (flag) {
    case MMFS_SB_U_INR:
        g_msb.inode_used += delta;
        g_msb.flags |= MMFS_SB_DIRTY;
        break;
    case MMFS_SB_U_SPACE:
        g_msb.space_used += delta;
        g_msb.flags |= MMFS_SB_DIRTY;
        break;
    }
    xlock_unlock(&g_msb.lock);
}

/*
 * ms->name : dentry name to stat
 * ms->ino  : dentry ino to stat (if ino > 0, use it; otherwise use ->name)
 */
int __mmfs_stat(u64 pino, struct mstat *ms)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    if (ms->ino == 0) {
        if (!ms->name) {
            hvfs_err(lib, "invalid argument for null name.\n");
            err = -EINVAL;
            goto out;
        }
        /* stat by self name, use pino */
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s", pino, ms->name);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "_IN_%ld / '%s' does not exist.\n",
                     pino, ms->name);
            err = -ENOENT;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            /* convert to ino */
            ms->ino = atol(rpy->str);
        }
        /* release resouce */
    out_free:
        freeReplyObject(rpy);
    }
    if (ms->ino > 0) {
        /* stat by self ino, ignore pino */
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s", ms->ino, MMFS_INODE_MD);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "_IN_%ld '_MD_' does not exist.\n", 
                     ms->ino);
            err = -ENOENT;
            goto out_free2;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_debug(lib, "_IN_%ld mdu: %s\n", ms->ino, rpy->str);
            /* convert mdu_string to mdu */
            ms->pino = pino;
            err = __str2mdu(rpy->str, &ms->mdu);
            if (err) {
                hvfs_err(lib, "Invalid MDU for %ld (%s)\n", ms->ino, rpy->str);
            }
        }
    out_free2:
        freeReplyObject(rpy);
    } else {
        err = -ENOENT;
    }
    
out:
    putRC(rc);

    return err;
}

int __mmfs_readlink(u64 pino, struct mstat *ms)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    if (ms->ino > 0) {
        /* stat by self ino, ignore pino */
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s", ms->ino, MMFS_INODE_SYMNAME);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "_IN_%ld '_SYMNAME_' does not exist.\n",
                     ms->ino);
            err = -ENOENT;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_debug(lib, "_IN_%ld metastore: %s\n", ms->ino, rpy->str);
            ms->arg = strdup(rpy->str);
        }
    out_free:
        freeReplyObject(rpy);
    }
out:
    putRC(rc);

    return err;
}

int __mmfs_create(u64 pino, struct mstat *ms, struct mdu_update *mu, u32 flags)
{
    char mstr[512];
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed \n");
        return -EINVAL;
    }

    __init_mdu(&ms->mdu, flags & __MMFS_CREATE_DIR);
    __pack_mdu(&ms->mdu, mu);
    __mdu2str(&ms->mdu, mstr, 1);
    hvfs_debug(lib, "CREATE MDU: (pino %ld)/%s -> ?,%s\n",
               pino, ms->name, mstr);

    /* Step 1: create a new inode hash table for self */
    if (flags & __MMFS_CREATE_INODE) {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_ %s %s %s %d",
                           g_ops[__MMFS_R_OP_CREATE_INODE].sha,
                           MMFS_INODE_MD,
                           mstr,
                           MMFS_INODE_VERSION,
                           0);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "_IN_* create for (pino %ld)/%s failed w/\n%s\n",
                     pino, ms->name, rpy->str);
            err = -EINVAL;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            /* parse the return mdu info */
            err = __str2mdu(rpy->str, &ms->mdu);
            if (err) {
                hvfs_err(lib, "_IN_* create for (pino %ld)/%s failed.\n",
                         pino, ms->name);
                err = -EINVAL;
                goto out_free;
            }
            hvfs_debug(lib, "_IN_%ld created ok (pino %ld)/%s.\n",
                       ms->mdu.ino, pino, ms->name);
            ms->ino = ms->mdu.ino;
            ms->pino = pino;
            __update_msb(MMFS_SB_U_INR, 1);
        }
    out_free:
        freeReplyObject(rpy);
    }
    if (err)
        goto out;

    if (flags & __MMFS_CREATE_SYMLINK) {
        hvfs_debug(lib, "CREATE SYMLINK from _IN_%ld to %s\n",
                   ms->ino, (char *)ms->arg);
        rpy = redisCommand(rc->rc, "hsetnx _IN_%ld %s %s",
                           ms->ino,
                           MMFS_INODE_SYMNAME,
                           (char *)ms->arg);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "_IN_%ld add SYMNAME %s failed w/ %s\n",
                     ms->ino, (char *)ms->arg, rpy->str);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out_clear;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 0) {
                err = -EEXIST;
                freeReplyObject(rpy);
                goto out_clear;
            }
        }
        freeReplyObject(rpy);
    }
    /* Step 2: insert a new entry to parent ino's hash table */
    if (flags & __MMFS_CREATE_DENTRY) {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s %ld",
                           g_ops[__MMFS_R_OP_CREATE_DENTRY].sha,
                           pino, ms->name, ms->ino);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s (_IN_%ld leaks)\n", 
                     rc->rc->errstr, ms->ino);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "create dentry for (pino %ld)/%s failed.\n",
                     pino, ms->name);
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out_clear;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 1) {
                /* this means target file name exists */
                err = -EEXIST;
                freeReplyObject(rpy);
                goto out_clear;
            }
        }
        freeReplyObject(rpy);
    }

out:
    putRC(rc);

    return err;
out_clear:
    {
        rpy = redisCommand(rc->rc, "del _IN_%ld", ms->ino);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "del _IN_%ld failed w/ %s\n",
                     ms->ino, rpy->str);
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 1) {
                hvfs_debug(lib, "del _IN_%ld ok.\n", ms->ino);
            } else {
                hvfs_warning(lib, "del _IN_%ld failed, no exists?\n", ms->ino);
            }
        }
        freeReplyObject(rpy);
    }
    goto out;
}

int __mmfs_create_root(struct mstat *ms, struct mdu_update *mu)
{
    char mstr[512];
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    __init_mdu(&ms->mdu, 1);
    __pack_mdu(&ms->mdu, mu);
    __mdu2str(&ms->mdu, mstr, 1);

    hvfs_info(lib, "CREATE ROOT MDU: (pino %d)/%s -> ?,%s\n",
              MMFS_ROOT_INO, ms->name, mstr);

    {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_ %s %s %s %d",
                           g_ops[__MMFS_R_OP_CREATE_INODE].sha,
                           MMFS_INODE_MD,
                           mstr,
                           MMFS_INODE_VERSION,
                           0);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "_IN_* create for (pino %d)/%s failed w/\n%s\n",
                     MMFS_ROOT_INO, ms->name, rpy->str);
            err = -EINVAL;
            goto out_free;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            /* parse the return mdu info */
            err = __str2mdu(rpy->str, &ms->mdu);
            if (err) {
                hvfs_err(lib, "_IN_* create for (pino %d)/%s failed.\n",
                         MMFS_ROOT_INO, ms->name);
                err = -EINVAL;
                goto out_free;
            }
            hvfs_debug(lib, "_IN_%ld created ok (pino %d)/%s.\n",
                       ms->mdu.ino, MMFS_ROOT_INO, ms->name);
            ms->ino = ms->mdu.ino;
            ms->pino = MMFS_ROOT_INO;
        }
    out_free:
        freeReplyObject(rpy);
    }

out:
    putRC(rc);

    return err;
}

int __mmfs_create_sb(struct mmfs_sb *msb)
{
    redisReply *rpy = NULL;
    int err = 0;
    struct redisConnection *rc = getRC();
    
    if (!msb->name || strlen(msb->name) == 0) {
        hvfs_err(lib, "Invalid file system name: null or empty string.\n");
        return -EINVAL;
    }
    
    hvfs_info(lib, "Begin create new file system named as '%s'.\n", 
              msb->name);

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }
    {
        rpy = redisCommand(rc->rc, "exists _MMFS_SB_%s", msb->name);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (!(rpy->type == REDIS_REPLY_INTEGER && rpy->integer == 0)) {
            hvfs_err(lib, "ERROR STATE of key '_MMFS_SB_%s', reject "
                     "create file system superblock.\n", msb->name);
            err = -EEXIST;
            goto out_free1;
        }
    out_free1:
        freeReplyObject(rpy);
        if (err)
            goto out;
    }
    {
        rpy = redisCommand(rc->rc, "hset _MMFS_SB_%s name %s", 
                           msb->name, msb->name);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (!(rpy->type == REDIS_REPLY_INTEGER && rpy->integer == 1)) {
            hvfs_err(lib, "ERROR STATE of key '_MMFS_SB_%s', reject "
                     "create file system superblock.\n", msb->name);
            err = -EEXIST;
            goto out_free2;
        }
    out_free2:
        freeReplyObject(rpy);
        if (err)
            goto out;
    }
    {
        rpy = redisCommand(rc->rc, "hmset _MMFS_SB_%s "
                           "space_quota %ld "
                           "space_used %ld "
                           "inode_quota %ld "
                           "inode_used %ld",
                           msb->name, 
                           msb->space_quota, msb->space_used,
                           msb->inode_quota, msb->inode_used);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_STATUS && 
            strcmp(rpy->str, "OK") == 0) {
            err = 0;
        } else {
            err = 1;
        }
        freeReplyObject(rpy);
    }

out:
    putRC(rc);

    return err;
}

int __mmfs_update_sb(struct mmfs_sb *msb)
{
    redisReply *rpy = NULL;
    int err = 0;
    struct redisConnection *rc = getRC();

    if (!msb->name || strlen(msb->name) == 0) {
        hvfs_err(lib, "Invalid file system name: null or empty string.\n");
        return -EINVAL;
    }

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }
    {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _MMFS_SB_%s %ld "
                           "%ld %ld %ld %ld %ld",
                           g_ops[__MMFS_R_OP_UPDATE_SB].sha,
                           msb->name,
                           msb->version,
                           msb->root_ino,
                           (s64)msb->d.space_quota,
                           (s64)msb->d.space_used,
                           (s64)msb->d.inode_quota,
                           (s64)msb->d.inode_used);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            err = EMMMETAERR;
            putRC(rc);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL) {
            hvfs_err(lib, "invalid arguments or version for update_sb.\n");
            err = -EINVAL;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ARRAY) {
            int i;
            
            for (i = 0; i < rpy->elements; i += 2) {
                char *f = rpy->element[i]->str;
                char *v = rpy->element[i + 1]->str;
                int l = strlen(f);

                if (strncmp(f, "version", l > 7 ? 7 : l) == 0)
                    msb->version = atol(v);
                else if (strncmp(f, "root_ino", l > 8 ? 8 : l) == 0)
                    msb->root_ino = atol(v);
                else if (strncmp(f, "space_quota", l > 11 ? 11 : l) == 0)
                    msb->space_quota = atol(v);
                else if (strncmp(f, "space_used", l > 10 ? 10 : l) == 0)
                    msb->space_used = atol(v);
                else if (strncmp(f, "inode_quota", l > 11 ? 11 : l) == 0)
                    msb->inode_quota = atol(v);
                else if (strncmp(f, "inode_used", l > 10 ? 10 : l) == 0)
                    msb->inode_used = atol(v);
            }
            memset(&msb->d, 0, sizeof(msb->d));
            msb->d.space_used = msb->space_used;
            msb->d.inode_used = msb->inode_used;
        }
        freeReplyObject(rpy);
    }
out:
    putRC(rc);

    return err;
}

int __mmfs_get_sb(struct mmfs_sb *msb)
{
    redisReply *rpy = NULL;
    int err= 0;
    struct redisConnection *rc = getRC();

    if (!msb->name || strlen(msb->name) == 0) {
        hvfs_err(lib, "Invalid file system name: null or empty string.\n");
        return -EINVAL;
    }

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }
    {
        rpy = redisCommand(rc->rc, "hgetall _MMFS_SB_%s",
                           msb->name);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            err = EMMMETAERR;
            putRC(rc);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ARRAY) {
            int i;

            if (rpy->elements <= 10) {
                err = -EINVAL;
            } else {
                for (i = 0; i < rpy->elements; i += 2) {
                    char *f = rpy->element[i]->str;
                    char *v = rpy->element[i + 1]->str;
                    int l = strlen(f);

                    if (strncmp(f, "version", l > 7 ? 7 : l) == 0)
                        msb->version = atol(v);
                    else if (strncmp(f, "root_ino", l > 8 ? 8 : l) == 0)
                        msb->root_ino = atol(v);
                    else if (strncmp(f, "space_quota", l > 11 ? 11 : l) == 0)
                        msb->space_quota = atol(v);
                    else if (strncmp(f, "space_used", l > 10 ? 10 : l) == 0)
                        msb->space_used = atol(v);
                    else if (strncmp(f, "inode_quota", l > 11 ? 11 : l) == 0)
                        msb->inode_quota = atol(v);
                    else if (strncmp(f, "inode_used", l > 10 ? 10 : l) == 0)
                        msb->inode_used = atol(v);
                }
                msb->flags = 0;
                memset(&msb->d, 0, sizeof(msb->d));
                msb->d.space_used = msb->space_used;
                msb->d.inode_used = msb->inode_used;
            }
        } else {
            err = -EINVAL;
        }
        freeReplyObject(rpy);
    }

out:
    putRC(rc);

    return err;
}

static inline int __update_inode(struct redisConnection *rc, redisReply *rpy, 
                                 struct mstat *ms)
{
    char mstr[512];
    struct mdu s = ms->mdu;
    int err = 0;
    
    if (!ms->ino || ms->ino != ms->mdu.ino) {
        hvfs_err(lib, "Invalid ino %ld (mdu %ld)\n",
                 ms->ino, ms->mdu.ino);
        err = -EINVAL;
        goto out;
    }

    s.version++;
    __mdu2str(&s, mstr, 0);

    /* Step 1: find by ino to update existing inode */
    rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s %s %s %ld %s %s",
                       g_ops[__MMFS_R_OP_UPDATE_INODE].sha,
                       ms->ino,
                       MMFS_INODE_MD,
                       mstr,
                       MMFS_INODE_VERSION,
                       (u64)ms->mdu.version,
                       MMFS_INODE_BLOCK,
                       (ms->arg ? ms->arg : "nil"));
    if (rpy == NULL) {
        hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
        err = EMMMETAERR;
        putRC(rc);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(lib, "find _IN_%ld failed w/ %s\n",
                 ms->ino, rpy->str);
        err = -EINVAL;
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        switch (rpy->integer) {
        case 1:
            /* updated */
            hvfs_debug(lib, "update inode _IN_%ld ok, version=%ld\n",
                       ms->ino, (u64)s.version);
            ms->mdu.version = s.version;
            break;
        case 0:
            /* failed */
            hvfs_warning(lib, "update inode _IN_%ld failed, "
                         "version mismatch (expect %ld).\n",
                         ms->ino, (u64)ms->mdu.version);
            err = -EAGAIN;
            break;
        default:
            /* failed */
            hvfs_warning(lib, "update inode _IN_%ld failed, key not "
                         "exists.\n ", ms->ino);
            err = -EINVAL;
        }
    }
    freeReplyObject(rpy);

out:
    return err;
}

int __mmfs_unlink(u64 pino, struct mstat *ms, u32 flags)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed \n");
        return -EINVAL;
    }

    hvfs_debug(lib, "DELETE dentry: (pino %ld)/%s\n",
               pino, ms->name);

    /* Step 1: delete the dentry in parent ino's hash table */
    if (flags & __MMFS_UNLINK_DENTRY) {
        rpy = redisCommand(rc->rc, "evalsha %s 1 _IN_%ld %s",
                           g_ops[__MMFS_R_OP_DELETE_DENTRY].sha,
                           pino, ms->name);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "delete dentry for (pino %ld)/%s failed w/ %s\n",
                     pino, ms->name, rpy->str);
            freeReplyObject(rpy);
            err = -EINVAL;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_INTEGER) {
            if (rpy->integer == 0) {
                /* this means we can NOT find the dentry */
                hvfs_err(lib, "find dentry for (pino %ld)/%s failed"
                         " to unlink (not exist)\n",
                         pino, ms->name);
                err = -ENOENT;
            } else {
                hvfs_err(lib, "find dentry for (pino %ld)/%s failed"
                         " to unlink (invalid reply %ld)\n",
                         pino, ms->name, (u64)rpy->integer);
                err = -EINVAL;
            }
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_debug(lib, "delete dentry for (pino %ld)/%s -> ino %s\n",
                       pino, ms->name, rpy->str);
            ms->mdu.ino = atol(rpy->str);
        }

        freeReplyObject(rpy);

        if (!ms->mdu.ino || ms->mdu.ino == 1) {
            hvfs_err(lib, "invalid ino %ld for (pino %ld)/%s\n",
                     ms->mdu.ino, pino, ms->name);
            err = -EINVAL;
            goto out;
        }
    }

    /* Step 2: delete the inode hash table for self */
    if (flags & __MMFS_UNLINK_INODE) {
        /* Step 2.1 read in the inode mdu to check nlink value */
        rpy = redisCommand(rc->rc, "hget _IN_%ld %s",
                           ms->mdu.ino, MMFS_INODE_MD);
        if (rpy == NULL) {
            hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
            putRC(rc);
            err = EMMMETAERR;
            goto out;
        }
        if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "_IN_%ld '_MD_' does not exist.\n",
                     ms->mdu.ino);
            err = -ENOENT;
            freeReplyObject(rpy);
            goto out;
        }
        if (rpy->type == REDIS_REPLY_STRING) {
            hvfs_debug(lib, "_IN_%ld mdu: %s\n", ms->mdu.ino, rpy->str);
            /* convert mdu_string to mdu */
            ms->ino = ms->mdu.ino;
            err = __str2mdu(rpy->str, &ms->mdu);
            if (err || ms->ino != ms->mdu.ino) {
                hvfs_err(lib, "Invalid MDU fro %ld (%s)\n", ms->ino, rpy->str);
                err = -EINVAL;
                freeReplyObject(rpy);
                goto out;
            }
        }

        if ((S_ISDIR(ms->mdu.mode) && ms->mdu.nlink > 2) ||
            (!S_ISDIR(ms->mdu.mode) && ms->mdu.nlink > 1)) {
            /* Step 2.2 call update inode to do nlink--*/
            ms->mdu.nlink--;
            err = __update_inode(rc, rpy, ms);
            if (err) {
                hvfs_err(lib, "call __update_inode _IN_%ld to nlink-- "
                         "failed w/ %d\n",
                         ms->ino, err);
            }
        } else {
            /* Step 2.2 do truely delete now */
            rpy = redisCommand(rc->rc, "del _IN_%ld",
                               ms->mdu.ino);
            if (rpy == NULL) {
                hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
                goto out;
            }
            if (rpy->type == REDIS_REPLY_ERROR) {
                hvfs_err(lib, "delete inode for (pino %ld)/%s _IN_%ld failed w/ %s\n",
                         pino, ms->name, ms->mdu.ino, rpy->str);
                freeReplyObject(rpy);
                goto out;
            }
            if (rpy->type == REDIS_REPLY_INTEGER) {
                if (rpy->integer == 1) {
                    hvfs_debug(lib, "delete inode for (pino %ld)/%s _IN_%ld ok\n",
                               pino, ms->name, ms->mdu.ino);
                    __update_msb(MMFS_SB_U_INR, -1);
                } else {
                    hvfs_err(lib, "delete inode for (pino %ld)/%s _IN_%ld bad (ignore)\n",
                             pino, ms->name, ms->mdu.ino);
                }
            }
            freeReplyObject(rpy);
        }
    }
out:
    putRC(rc);

    return err;
}

/* Return: 1 -> ok, 0 -> not empty or error
 */
int __mmfs_is_empty_dir(u64 dino)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed \n");
        return -EINVAL;
    }

    /* Step 1: find by ino to check any existing dentries */
    rpy = redisCommand(rc->rc, "hlen _IN_%ld", dino);
    if (rpy == NULL) {
        hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
        putRC(rc);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(lib, "find _IN_%ld failed w/ %s\n",
                 dino, rpy->str);
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        if (rpy->integer == 2 || rpy->integer == 1) {
            /* ok, do NOT test further more */
            err = 1;
        } else if (rpy->integer == 0) {
            /* invalid INODE or not exist INODE */
        } else if (rpy->integer > 2) {
            /* not empty dir */
        } else {
            /* invalid reply */
        }
    }
    freeReplyObject(rpy);
out:
    putRC(rc);

    return err;
}

int __mmfs_update_inode(struct mstat *ms, struct mdu_update *mu)
{
    redisReply *rpy = NULL;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    if (mu != NULL) {
        __pack_mdu(&ms->mdu, mu);
    }

    err = __update_inode(rc, rpy, ms);

    putRC(rc);

    return err;
}

int __mmfs_linkadd(struct mstat *ms, s32 nlink)
{
    int err = 0;

    ms->mdu.nlink += nlink;

    err = __mmfs_update_inode(ms, NULL);
    if (err) {
        hvfs_err(lib, "call __mmfs_update_inode on _IN_%ld failed w/ %d\n",
                 ms->ino, err);
        goto out;
    }
out:
    return err;
}

int __mmfs_fread(struct mstat *ms, void **data, u64 off, u64 size)
{
    redisReply *rpy = NULL;
    size_t rlen = 0;
    int err = 0;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    if (ms->ino <= 0) {
        hvfs_err(lib, "invalid ino %ld provided\n", ms->ino);
        err = -EINVAL;
        goto out;
    }

    if (S_ISDIR(ms->mdu.mode)) {
        hvfs_err(lib, "directory (ino %ld) has no blocks to read\n", ms->ino);
        err = -EISDIR;
        goto out;
    }

    if (off + size > ms->mdu.size) {
        if (off > ms->mdu.size) {
            hvfs_debug(lib, "Read offset across the boundary (%ld vs %ld)\n",
                       off, ms->mdu.size);
            err = -EFBIG;
            goto out;
        } else {
            /* Convention: for fuse client, it always read for some pages, we
             * should truncate the size to validate range */
            size = ms->mdu.size - off;
        }
    }

    hvfs_debug(lib, "__mmfs_fread(%ld) off=%ld, size=%ld, mdu.size=%ld\n",
               ms->ino, (u64)off, (u64)size, ms->mdu.size);

    rpy = redisCommand(rc->rc, "hget _IN_%ld %s", ms->ino, MMFS_INODE_BLOCK);
    if (rpy == NULL) {
        hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
        putRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_NIL || rpy->type == REDIS_REPLY_ERROR) {
        hvfs_warning(lib, "_IN_%ld does not exist or MM error\n",
                     ms->ino);
        if (ms->mdu.size > 0) {
            err = -ENOENT;
        } else {
            err = -EFBIG;
        }
        goto out_free;
    }
    if (rpy->type == REDIS_REPLY_STRING) {
        void *buf = NULL;
        
        err = mmcc_get(rpy->str, &buf, &rlen);
        if (err) {
            hvfs_err(lib, "_IN_%ld block get(%s) failed w/ %d\n",
                     ms->ino, rpy->str, err);
            goto out_free;
        }
        if (!*data) {
            *data = xzalloc(size);
            if (!*data) {
                err = -ENOMEM;
                xfree(buf);
                goto out_free;
            }
        }
        if (rlen - off < 0) {
            hvfs_warning(lib, "_IN_%ld block size %ld - off %ld < 0\n",
                         ms->ino, rlen, off);
            err = 0;
        } else if (off + size > rlen) {
            hvfs_warning(lib, "_IN_%ld block size %ld < request size %ld\n",
                         ms->ino, rlen, size);
            memcpy(*data, buf + off, rlen - off);
            err = rlen - off;
        } else {
            memcpy(*data, buf + off, size);
            err = size;
        }
        xfree(buf);
    }

out_free:
    freeReplyObject(rpy);
out:
    putRC(rc);

    return err;
}

int __mmfs_fwrite(struct mstat *ms, u32 flag, void *data, u64 size)
{
    redisReply *rpy = NULL;
    char *set = NULL, *p, name[64], key[256], *info = NULL;
    MD5_CTX mdContext;
    int err = 0, i;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    if (ms->ino <= 0) {
        hvfs_err(lib, "invalid ino %ld provided\n", ms->ino);
        err = -EINVAL;
        goto out;
    }
    if (S_ISDIR(ms->mdu.mode)) {
        hvfs_err(lib, "directory (ino %ld) has no blocks to write\n", ms->ino);
        err = -EISDIR;
        goto out;
    }

    /* write the content to MMServer, and generate key */
    err = __mmfs_gset(ms->pino, &set);
    if (err) {
        goto out;
    }
    MD5Init(&mdContext);
    MD5Update(&mdContext, (unsigned char *)data, size);
    MD5Final(&mdContext);
    
    for (i = 0, p = name; i < 16; i++) {
        p += sprintf(p, "%02x", mdContext.digest[i]);
    }
    
    snprintf(key, 255, "%s@%s", set, name);

    info = mmcc_put(key, data, size);
    if (!info) {
        hvfs_err(lib, "_IN_%ld block put failed w/ %d\n",
                 ms->ino, err);
        goto out_free;
    }
    hvfs_debug(lib, "_IN_%ld block put key=%s info=%s\n",
               ms->ino, key, info);

    rpy = redisCommand(rc->rc, "hset _IN_%ld %s %s", 
                       ms->ino,
                       MMFS_INODE_BLOCK,
                       key);
    if (rpy == NULL) {
        hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
        putRC(rc);
        err = EMMMETAERR;
        goto out_free2;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(lib, "_IN_%ld ('%s') does not exist or MM error\n",
                 ms->ino, ms->name);
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out_free2;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        if (rpy->integer == 1) {
            /* not exist yet */
            hvfs_debug(lib, "_IN_%ld '%s' block set    to %s\n",
                       ms->ino, ms->name, key);
        } else {
            /* updated */
            hvfs_debug(lib, "_IN_%ld '%s' block update to %s\n",
                       ms->ino, ms->name, key);
        }
    }
    freeReplyObject(rpy);
    
out_free2:
    xfree(info);
out_free:
    xfree(set);
out:
    putRC(rc);

    return err;
}

int __mmfs_fwritev(struct mstat *ms, u32 flag, struct iovec *iov, int iovlen)
{
    redisReply *rpy = NULL;
    char *set = NULL, *p, name[64], key[256], *info = NULL;
    MD5_CTX mdContext;
    int err = 0, i;

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    if (ms->ino <= 0) {
        hvfs_err(lib, "invalid ino %ld provided\n", ms->ino);
        err = -EINVAL;
        goto out;
    }
    if (S_ISDIR(ms->mdu.mode)) {
        hvfs_err(lib, "directory (ino %ld) has no blocks to write\n", ms->ino);
        err = -EISDIR;
        goto out;
    }

    /* write the content to MMServer, and generate key */
    err = __mmfs_gset(ms->pino, &set);
    if (err) {
        goto out;
    }
    MD5Init(&mdContext);
    for (i = 0; i < iovlen; i++) {
        MD5Update(&mdContext, (unsigned char *)iov[i].iov_base, iov[i].iov_len);
    }
    MD5Final(&mdContext);
    
    for (i = 0, p = name; i < 16; i++) {
        p += sprintf(p, "%02x", mdContext.digest[i]);
    }
    
    snprintf(key, 255, "%s@%s", set, name);

    info = mmcc_put_iov(key, iov, iovlen);
    if (!info) {
        hvfs_err(lib, "_IN_%ld block put failed w/ %d\n",
                 ms->ino, err);
        goto out_free;
    }
    hvfs_debug(lib, "_IN_%ld block put key=%s info=%s\n",
               ms->ino, key, info);

    rpy = redisCommand(rc->rc, "hset _IN_%ld %s %s", 
                       ms->ino,
                       MMFS_INODE_BLOCK,
                       key);
    if (rpy == NULL) {
        hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
        putRC(rc);
        err = EMMMETAERR;
        goto out_free2;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(lib, "_IN_%ld ('%s') does not exist or MM error\n",
                 ms->ino, ms->name);
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out_free2;
    }
    if (rpy->type == REDIS_REPLY_INTEGER) {
        if (rpy->integer == 1) {
            /* not exist yet */
            hvfs_debug(lib, "_IN_%ld block set to %s\n",
                       ms->ino, key);
        } else {
            /* updated */
            hvfs_debug(lib, "_IN_%ld block update to %s\n",
                       ms->ino, key);
        }
    }
    freeReplyObject(rpy);
    
out_free2:
    xfree(info);
out_free:
    xfree(set);
out:
    putRC(rc);

    return err;
}

int __mmfs_readdir(mmfs_dir_t *dir)
{
    struct mstat ms = {0,};
    redisReply *rpy = NULL;
    int err = 0, idx;

    if (dir->dino <= 0) {
        hvfs_err(lib, "invalid ino %ld provided\n", dir->dino);
        return -EINVAL;
    }

    ms.ino = dir->dino;
    err = __mmfs_stat(0, &ms);
    if (err) {
        hvfs_err(lib, "__mmfs_stat(%ld) failed w/ %d\n",
                 dir->dino, err);
        return err;
    }

    if (!S_ISDIR(ms.mdu.mode)) {
        hvfs_err(lib, "_IN_%ld is not a directory\n", dir->dino);
        return -ENOTDIR;
    }

    struct redisConnection *rc = getRC();

    if (!rc) {
        hvfs_err(lib, "getRC() failed\n");
        return -EINVAL;
    }

    /* hscan the dentries */
    rpy = redisCommand(rc->rc, "hscan _IN_%ld %s count 100",
                       dir->dino, dir->cursor);
    if (rpy == NULL) {
        hvfs_err(lib, "read from MM Meta failed: %s\n", rc->rc->errstr);
        putRC(rc);
        err = EMMMETAERR;
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ERROR) {
        hvfs_err(lib, "_IN_%ld does not exist or MM error\n",
                 dir->dino);
        err = -ENOENT;
        freeReplyObject(rpy);
        goto out;
    }
    if (rpy->type == REDIS_REPLY_ARRAY && rpy->elements == 2 &&
        rpy->element[1]->type == REDIS_REPLY_ARRAY) {
        int i = 0, j = 0, tlen = 0, clen = 0;

        dir->cursor = strdup(rpy->element[0]->str);
        
        for (i = 0; i < rpy->element[1]->elements; i += 2) {
            char *f = rpy->element[1]->element[i]->str;
            char *v = rpy->element[1]->element[i + 1]->str;
            int l = strlen(f);

            if (l == MMFS_I_XSIZE && strncmp(f, "__", 2) == 0) {
                /* do more check */
                if (strncmp(f, MMFS_INODE_NAME, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_MD, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_SYMNAME, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_VERSION, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_BLOCK, l) == 0) {
                    continue;
                } else if (strncmp(f, MMFS_INODE_CHUNKNR, l) == 0) {
                    continue;
                }
            }
            /* ok, do record now */
            clen = sizeof(struct dentry_info) + l;
            void *t = xrealloc(dir->di, tlen + clen);
            if (!t) {
                hvfs_err(lib, "xrealloc(%d) failed\n", tlen + clen);
                err = -ENOMEM;
                freeReplyObject(rpy);
                xfree(dir->di);
                dir->di = NULL;
                goto out;
            }
            dir->di = t;
            struct dentry_info *di = (t + tlen);

            di->ino = atoi(v);
            di->mode = 0;
            di->namelen = l;
            memcpy(di->name, f, l);
            tlen += clen;
            j++;
        }
        dir->csize = j;
    }
    freeReplyObject(rpy);
out:
    putRC(rc);

    /* get inode mode for each dentry now */
    struct dentry_info *di = dir->di;

    for (idx = 0; idx < dir->csize; idx++) {
        ms.ino = di->ino;
        err = __mmfs_stat(0, &ms);
        if (err) {
            hvfs_err(lib, "__mmfs_stat() _IN_%ld to get mode failed w/ %d\n",
                     ms.ino, err);
        } else
            di->mode = ms.mdu.mode;
        di = (void *)di + sizeof(*di) + di->namelen;
    }

    return err;
}
