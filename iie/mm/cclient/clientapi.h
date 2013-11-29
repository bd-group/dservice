

int init(char *uris);
char *put(char *key, void *content, size_t len);
int get(char *key, void **buffer, size_t *len);
