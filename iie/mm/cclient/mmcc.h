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

#include <unistd.h>
/*
 * Provide url list, splited by ';'
 */
int mmcc_init(char *uris);

/*
 * Return the fast lookup info, it can be used as 'key' in get()
 */
char *mmcc_put(char *key, void *content, size_t len);

/*
 * Caller should free the allocated memory by 'free'
 */
int mmcc_get(char *key, void **buffer, size_t *len);

/*
 * ERROR numbers
 */
#define EMMMETAERR              -1025
#define EMMNOTFOUND             -1026
#define EMMCONNERR              -1027
#define EMMINVAL                -1028
#define EMMNOMEM                -1029

/* Advanced Using, use only if you know it
 */
struct key2info
{
    char *key;
    char *info;
};

struct key2info *get_k2i(char *set, int *nr);

#ifdef __cplusplus
}
#endif 

#endif
