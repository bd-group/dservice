#include "mmcc.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <netinet/in.h> 
#include "hiredis.h"
#include <netdb.h>
#include <unistd.h>
#include "hvfs_u.h"
#include "memory.h"

#define HVFS_TRACING
#include "tracing.h"

#define DUPNUM 2

extern char *g_uris;
extern char *redisHost;
extern int redisPort;
extern atomic_t g_master_connect_err;
extern unsigned int hvfs_mmcc_tracing_flags;

struct redisConnection;

int client_config(mmcc_config_t *);
int client_init();
int client_fina();
int update_mmserver();
int get_mm_object(char* set, char* md5, void **buf, size_t* length);
int search_mm_object(char *infos, void **buf, size_t *length);
struct redisConnection *getRC();
void putRC(struct redisConnection *rc);
int del_mm_set(char *set);

static inline void __fetch_from_sentinel(char *uris)
{
    char *dup = strdup(uris), *p, *n = NULL, *q, *m = NULL;
    int inited = 0;

    p = dup;
    do {
        p = strtok_r(p, ";", &n);
        if (p) {
            /* parse sentinel server */

            char *rh = NULL;
            int rp;

            q = p;
            q = strtok_r(q, ":", &m);
            if (q) {
                rh = strdup(p);
            } else {
                hvfs_err(mmcc, "parse hostname failed.\n");
                continue;
            }
            q = strtok_r(NULL, ":", &m);
            if (q) {
                rp = atol(q);
            } else {
                hvfs_err(mmcc, "parse port failed.\n");
                continue;
            }
            /* ok, connect to sentinel here */
            {
                redisContext *rc = redisConnect(rh, rp);
                redisReply *r = NULL;

                if (rc->err) {
                    hvfs_err(mmcc, "can't connect to redis sentinel at %s:%d %s\n",
                             rh, rp, rc->errstr);
                    continue;
                }
                r = redisCommand(rc, "SENTINEL get-master-addr-by-name mymaster");
                if (r == NULL) {
                    hvfs_err(mmcc, "get master failed.\n");
                    goto free_rc;
                }
                if (r->type == REDIS_REPLY_NIL || r->type == REDIS_REPLY_ERROR) {
                    hvfs_warning(mmcc, "SENTINEL get master from %s:%d failed, "
                                 "try next one\n", rh, rp);
                }
                if (r->type == REDIS_REPLY_ARRAY) {
                    if (r->elements != 2) {
                        hvfs_warning(mmcc, "Invalid SENTINEL reply? len=%ld\n",
                                     r->elements);
                    } else {
                        if (redisHost == NULL || redisPort == -1) {
                            redisHost = strdup(r->element[0]->str);
                            redisPort = atoi(r->element[1]->str);
                            hvfs_info(mmcc, "OK, got MM Master Server %s:%d\n",
                                      redisHost, redisPort);
                            inited = 1;
                        } else {
                            if ((strcmp(r->element[0]->str, redisHost) != 0) ||
                                atoi(r->element[1]->str) != redisPort) {
                                hvfs_info(mmcc, "Change MM Master Server from "
                                          "%s:%d to %s:%s\n",
                                          redisHost, redisPort,
                                          r->element[0]->str,
                                          r->element[1]->str);
                                xfree(redisHost);
                                redisHost = strdup(r->element[0]->str);
                                redisPort = atoi(r->element[1]->str);
                                inited = 1;
                            }
                        }
                    }
                }
                freeReplyObject(r);
            free_rc:                
                redisFree(rc);
            }
            xfree(rh);
            if (inited)
                break;
        } else
            break;
    } while (p = NULL, 1);

    xfree(dup);
}

static int init_with_sentinel(char *uris)
{
    int err = 0;

    err = client_init();
    if (err) {
        hvfs_err(mmcc, "client_init() failed w/ %d\n", err);
        goto out;
    }

    __fetch_from_sentinel(uris);
    g_uris = strdup(uris);

out:
    return err;
}

static int init_standalone(char *uris)
{
    char *dup = strdup(uris), *p, *n = NULL;
	char *rh = NULL;
	int rp;
    int err = 0;
    
    err = client_init();
    if (err) {
        hvfs_err(mmcc, "client_init() failed w/ %d\n", err);
        goto out;
    }

    p = dup;
    p = strtok_r(p, ":", &n);
    if (p) {
        rh = strdup(p);
    } else {
        hvfs_err(mmcc, "parse hostname failed.\n");
        err = EMMINVAL;
        goto out_free;
    }
    p = strtok_r(NULL, ":", &n);
    if (p) {
        rp = atol(p);
    } else {
        hvfs_err(mmcc, "parse port failed.\n");
        err = EMMINVAL;
        goto out_free;
    }

    hvfs_info(mmcc, "Connect to redis server: %s\n", uris);
    
    redisHost = rh;
    redisPort = rp;

    {
        struct redisConnection *rc = getRC();
        
        if (!rc) {
            hvfs_err(mmcc, "can't connect to redis at %s:%d\n", rh, rp);
            err = EMMMETAERR;
            goto out_free;
        }
        putRC(rc);
    }

    xfree(dup);

out:
    return err;

out_free:
    xfree(dup);
    xfree(rh);

    return err;
}

/* URIs:
 * standalone redis server -> STA://host:port;host:port
 * sentinel   redis server -> STL://host:port;host:port
 */
int mmcc_init(char *uris)
{
    if (!uris)
        return EMMINVAL;
    
    g_client_tick = time(NULL);

    if (strstr(uris, "STL://")) {
        int err = 0;
        struct redisConnection *rc = NULL;

        err = init_with_sentinel(uris + 6);
        if (err) {
            hvfs_err(mmcc, "init_with_sentinel(%s) failed w/ %d\n",
                     uris, err);
            return err;
        }
        
        rc = getRC();
        if (!rc) {
            hvfs_err(mmcc, "getRC() failed\n");
            return EINVAL;
        }

        /* FIXME: do auto config */
        putRC(rc);

        return err;
    } else if (strstr(uris, "STA://")) {
        return init_standalone(uris + 6);
    } else {
        return init_standalone(uris);
    }
}

int mmcc_fina()
{
    int err = 0;

    err = client_fina();
    if (err) {
        hvfs_err(mmcc, "client_fina() failed w/ %d\n", err);
        goto out;
    }

out:
    return err;
}

static inline void __parse_token(char *key, int *m, int *n)
{
    *m = *n = 0;
    
    while (*key != '\0') {
        if (*key == '#') {
            *m += 1;
        } else if (*key == '@') {
            *n += 1;
        }
        key++;
    }
}

int mmcc_get(char *key, void **buffer, size_t *len)
{
    char *dup = strdup(key);
    char *p = dup, *n = NULL;
    int err = 0, sharpnr, atnr;

    __parse_token(key, &sharpnr, &atnr);
    if (unlikely(sharpnr > 0)) {
        err = search_mm_object(dup, buffer, len);
    } else if (atnr == 1) {
        char *set = strtok_r(p, "@", &n);
        char *md5 = strtok_r(NULL, "@", &n);

        err = get_mm_object(set, md5, buffer, len);
    } else {
        err = search_mm_object(dup, buffer, len);
    }
    
    xfree(dup);

    return err;
}

int mmcc_del_set(char *set)
{
    int err = 0;

    if (set == NULL || strlen(set) == 0) {
        err = -EINVAL;
        goto out;
    }
    
    err = del_mm_set(set);
    if (err) {
        hvfs_err(mmcc, "del_mm_set(%s) failed w/ %d\n",
                 set, err);
        goto out;
    }
out:
    return err;
}

void __master_connect_fail_check()
{
#define MASTER_FAIL_CHECK       10
    atomic_inc(&g_master_connect_err);
    if (atomic_read(&g_master_connect_err) >= MASTER_FAIL_CHECK) {
        if (g_uris) {
            __fetch_from_sentinel(g_uris);
            atomic_set(&g_master_connect_err, MASTER_FAIL_CHECK >> 2);
        }
    }
}

int mmcc_config(mmcc_config_t *mc)
{
    return client_config(mc);
}

