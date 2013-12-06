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

extern char *redisHost;
extern int redisPort;
extern unsigned int hvfs_mmcc_tracing_flags;

struct redisConnection;

int client_init();
int update_mmserver();
int get_mm_object(char* set, char* md5, void **buf, size_t* length);
int search_mm_object(char *infos, void **buf, size_t *length);
struct redisConnection *getRC();
void putRC(struct redisConnection *rc);

static int init_with_sentinel(char *uris)
{
    int err = 0, inited = 0;

    err = client_init();
    if (err) {
        hvfs_err(mmcc, "client_init() failed w/ %d\n", err);
        goto out;
    }

    char *dup = strdup(uris), *p, *n = NULL, *q, *m = NULL;

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
                    hvfs_err(mmcc, "can't connect to redis at %s:%d %s\n",
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
                        redisHost = strdup(r->element[0]->str);
                        redisPort = atoi(r->element[1]->str);
                        hvfs_info(mmcc, "OK, got MMM Server %s:%d\n",
                                  redisHost, redisPort);
                        inited = 1;
                    }
                }
                freeReplyObject(r);
            free_rc:                
                redisFree(rc);
            }
            if (inited)
                break;
        } else
            break;
    } while (p = NULL, 1);
    
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
int init(char *uris)
{
    if (!uris)
        return EMMINVAL;
    
    if (strstr(uris, "STL://")) {
        return init_with_sentinel(uris + 6);
    } else if (strstr(uris, "STA://")) {
        return init_standalone(uris + 6);
    } else {
        return init_standalone(uris);
    }
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

int get(char *key, void **buffer, size_t *len)
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
