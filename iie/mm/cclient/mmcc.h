/*
 *
 * Header file for MM C Client.
 *
 * Any problems please contact macan/zhaoyang @ IIE
 *
 */

#ifndef __MMCC_H__
#define __MMCC_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/uio.h>
#include <unistd.h>
/*
 * Provide url list, splited by ';'
 */
int mmcc_init(char *uris);

/*
 * finalize mmcc client
 */
int mmcc_fina();
    
/*
 * Return the fast lookup info, it can be used as 'key' in get()
 */
char *mmcc_put(char *key, void *content, size_t len);

char *mmcc_put_iov(char *key, struct iovec *iov, int iovlen);
    

/*
 * Caller should free the allocated memory by 'free'
 */
int mmcc_get(char *key, void **buffer, size_t *len);


/*
 * Delete the whole set, release storage space
 */
int mmcc_del_set(char *set);


/*
 * ERROR numbers
 */
#define EMMMETAERR              -1025
#define EMMNOTFOUND             -1026
#define EMMCONNERR              -1027
#define EMMINVAL                -1028
#define EMMNOMEM                -1029
#define EREDIRECT               -1030

/* Advanced Using, use only if you know it
 */
struct key2info
{
    char *key;
    char *info;
};

struct key2info *get_k2i(char *set, int *nr);

void mmcc_debug_mode(int enable);

extern time_t g_client_tick;

typedef void *(*__timer_cb)(void *);

typedef struct
{
    __timer_cb tcb;
    int ti;
} mmcc_config_t;

int mmcc_config(mmcc_config_t *);

#ifdef __cplusplus
}
#endif 

#endif
