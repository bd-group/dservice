#include "mmfs.h"

/* please use environment variables to pass MMFS specific values */
int main(int argc, char *argv[])
{
    char *value, *uris = NULL, *namespace = NULL, *rootdir = NULL;
    int noatime = -1, nodiratime = -1, ttl = -1, debug = 0, perm = -1;
    int err = 0;

    value = getenv("noatime");
    if (value) {
        noatime = atoi(value);
    }
    value = getenv("nodiratime");
    if (value) {
        nodiratime = atoi(value);
    }
    value = getenv("uris");
    if (value) {
        uris = strdup(value);
    }
    value = getenv("namespace");
    if (value) {
        namespace = strdup(value);
    }
    value = getenv("rootdir");
    if (value) {
        rootdir = strdup(value);
    }
    value = getenv("perm");
    if (value) {
        perm = atoi(value);
    }
    value = getenv("ttl");
    if (value) {
        ttl = atoi(value);
    }
    value = getenv("debug");
    if (value) {
        debug = atoi(value);
        mmfs_debug_mode(debug);
    }

    if (noatime >= 0 ||
        nodiratime >= 0 ||
        ttl >= 0 || perm >= 0 || uris || namespace) {
        /* reset minor value to default value */
        if (noatime < 0)
            noatime = 1;
        if (nodiratime < 0)
            nodiratime = 1;
        if (ttl < 0)
            ttl = 5;
        if (perm < 0)
            perm = 0;
        if (!uris) {
            hvfs_err(lib, "ENV 'uris' should be set.\n");
            return EINVAL;
        }
        if (!namespace) {
            namespace = "default";
        }
        mmfs_fuse_mgr.inited = 1;
        mmfs_fuse_mgr.sync_write = 0;
        mmfs_fuse_mgr.noatime = (noatime > 0 ? 1 : 0);
        mmfs_fuse_mgr.nodiratime = (nodiratime > 0 ? 1 : 0);
        mmfs_fuse_mgr.perm = (perm > 0 ? 1 : 0);
        mmfs_fuse_mgr.ttl = ttl;
        mmfs_fuse_mgr.uris = uris;
        mmfs_fuse_mgr.namespace = namespace;
    }

    /* reconstruct the MMFS arguments */
    hvfs_info(lib, "This MMFS client only implements a %sNon-ATOMIC%s "
              "rename.\n",
              MMFS_COLOR_RED, MMFS_COLOR_END);

    /* set page size of internal page cache */
    value = getenv("ps");
    if (value) {
        size_t ps = atol(value);

        g_pagesize = getpagesize();
        if (ps > g_pagesize) {
            g_pagesize = PAGE_ROUNDUP(ps, g_pagesize);
        } else
            g_pagesize = 0;
        hvfs_info(lib, "MMFS client data cache pagesize=%ld\n", g_pagesize);
    }

#if FUSE_USE_VERSION >= 26
    err = fuse_main(argc, argv, &mmfs_ops, NULL);
#else
    err = fuse_main(argc, argv, &mmfs_ops);
#endif
    if (err) {
        hvfs_err(lib, "fuse_main() failed w/ %s\n",
                 strerror(err > 0 ? err : -err));
        goto out;
    }
out:
    xfree(uris);
    xfree(namespace);
    xfree(rootdir);
    
    return err;
}
