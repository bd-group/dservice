#include "mmcc.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <semaphore.h>

static int g_tget_stop = 0;
static sem_t tget_sem;

struct tget_args
{
    char *set;
    struct key2info *ki;
    double begin, end;
    int gnr, id;
    size_t len;
};

static void *__tget(void *args)
{
    struct tget_args *ta = (struct tget_args *)args;
    struct timeval tv;
    char *buffer;
    int i, err;
    size_t len, tlen = 0;

    gettimeofday(&tv, NULL);
    ta->begin = (double)(tv.tv_sec * 1000000 + tv.tv_usec);

    for (i = 0; i < ta->gnr; i++) {
        char key[256];

        sprintf(key, "%s@%s", ta->set, ta->ki[i].key);
        err = mmcc_get(key, (void **)&buffer, &len);
        if (err) {
            printf("get(%s) failed w/ %d\n", key, err);
        } else {
            tlen += len;
            free(buffer);
        }
    }
    gettimeofday(&tv, NULL);
    ta->end = (double)(tv.tv_sec * 1000000 + tv.tv_usec);
    
    printf("TGID=%d gnr %d %.4f us, GPS %.4f, tlen %ld.\n", 
           ta->id, ta->gnr, (ta->end - ta->begin),
           (ta->gnr * 1000000.0 / (ta->end - ta->begin)), (long)tlen);
    ta->len = tlen;

    sem_post(&tget_sem);
    
    pthread_exit(NULL);
}

int thread_get(char *set, int gnr, int tnr)
{
    pthread_t threads[tnr];
    struct tget_args ta[tnr];
    struct key2info *ki;
    struct timeval tv;
    double begin, end;
    int i, anr = 0, err;
    size_t tlen = 0;

    sem_init(&tget_sem, 0, 0);
    
    /* get valid keys */
    ki = get_k2i(set, &anr);
    if (!ki) {
        printf("get_k2i(%s) failed.\n", set);
        return -1;
    }
    if (gnr > anr)
        gnr = anr;

    gettimeofday(&tv, NULL);
    begin = tv.tv_sec + tv.tv_usec / 1000000.0;
    for (i = 0; i < tnr; i++) {
        ta[i].id = i;
        ta[i].set = set;
        ta[i].ki = ki + i * (gnr / tnr);
        ta[i].gnr = gnr / tnr;

        err = pthread_create(&threads[i], NULL, __tget, &ta[i]);
        if (err) {
            perror("pthread_create");
            g_tget_stop = 1;
            break;
        }
    }

    if (g_tget_stop)
        return err;
    
    for (i = 0; i < tnr; i++) {
        sem_wait(&tget_sem);
    }
    
    for (i = 0; i < tnr; i++) {
        pthread_join(threads[i], NULL);
        tlen += ta[i].len;
    }
    gettimeofday(&tv, NULL);
    end = tv.tv_sec + tv.tv_usec / 1000000.0;
    
    for (i = 0; i < anr; i++) {
        free(ki[i].key);
        free(ki[i].info);
    }
    free(ki);

    printf("TGET tlen %ldB tlat %lf s, avg lat %lf ms\n", (long)tlen, 
           (end - begin), (end - begin) / anr * 1000.0);

    return 0;
}

int main(int argc, char *argv[])
{
    int err = 0;
    char *buffer = NULL, *key = "default@206dd46198a06e912e34c9793afb9ce3";
    size_t len = 0;
    
    printf("MMCC Demo\n");
    if (argc < 2) {
        printf("Usage: demo rul\n");
        goto out;
    }

    err = mmcc_init(argv[1]);
    if (err) {
        printf("init() failed w/ %d\n", err);
        goto out;
    }

#if 0
    err = get(key, (void **)&buffer, &len);
    if (err) {
        printf("get() failed w/ %d\n", err);
        goto out;
    }

    printf("Get key %s => len %ld\n", key, len);
    free(buffer);

    key = "1@default@2@0@60320000@32000@a";

    err = get(key, (void **)&buffer, &len);
    if (err) {
        printf("get() failed w/ %d\n", err);
        goto out;
    }

    printf("Get key %s => len %ld\n", key, len);
    free(buffer);

    key = "1@default@1@0@0@193247@c#1@default@1@0@0@193247@d";

#if 0
    {
        int n = 120;
        do {
            n = sleep(n);
        } while (n--);
    }
#endif

    err = get(key, (void **)&buffer, &len);
    if (err) {
        printf("get() failed w/ %d\n", err);
        goto out;
    }

    printf("Get key %s => len %ld\n", key, len);
    free(buffer);
#endif

    err = thread_get("default", 100000, 500);
    if (err) {
        printf("thread_get() failed w/ %d\n", err);
    }

out:
    return err;
}
