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

int init(char *uris)
{
    int err = 0;
    
    err = client_init();
    if (err) {
        hvfs_err(mmcc, "client_init() failed w/ %d\n", err);
        goto out;
    }

    char *dup = strdup(uris), *p, *n;
	char *rh = NULL;
	int rp;

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

int countChInStr(char* str,char c)
{
    int n = 0;
    for(;*str != '\0';str++)
    {
        if(*str == c)
            n++;
    }
    return n;
}

void __parse_token(char *key, int *m, int *n)
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
    char *p = dup, *n;
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

int get_old(char *key, void **buffer, size_t *len)
{
	int n = countChInStr(key,'@') + countChInStr(key,'#');
    char *p = strdup(key), *np;
    int err;

	if(n == 1) {
		char *set = strtok_r(p,"@", &np);
		char *md5 = strtok_r(NULL,"@", &np);
        err = get_mm_object(set,md5,buffer,len);
	} else if((n+1)%8 == 0) {
        err = search_mm_object(key,buffer,len);
	} else {
		printf("wrong format of key:%s\n",key);
        err = EMMINVAL;
	}
    xfree(p);

    return err;
}


