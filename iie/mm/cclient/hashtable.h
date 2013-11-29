#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define HASHSIZE 16

typedef struct _node{
    char *key;
    int val;
    struct _node *next;
}node;

static node* hashtab[HASHSIZE];
static int curIndex = 0;
static node* curNode = NULL;

void hash_init(){
    int i;
    for(i=0;i<HASHSIZE;i++)
        hashtab[i]=NULL;
}


unsigned int hash_hash(char *key){
    unsigned int h=0;
    for(;*key;key++)
        h=*key+h*31;
    return h%HASHSIZE;
}

node* hash_lookup(char *key){
    unsigned int hi=hash_hash(key);
    node* np=hashtab[hi];
    for(;np!=NULL;np=np->next){
        if(!strcmp(np->key,key))
            return np;
    }
    
    return NULL;
}

int hash_get(char* name){
    node* n=hash_lookup(name);
    if(n==NULL)
        return -1;
    else
        return n->val;
}

//复制一个字符串
char* m_strdup(char *o){
    int l=strlen(o)+1;
    char *ns=(char*)malloc(l*sizeof(char));
    strcpy(ns,o);
    if(ns==NULL)
        return NULL;
    else
        return ns;
}

int hash_put(char* key,int val){
    unsigned int hi;
    node* np;
    if((np=hash_lookup(key))==NULL){
        hi=hash_hash(key);
        np=(node*)malloc(sizeof(node));
        if(np==NULL)
            return 0;
        np->key=m_strdup(key);
        if(np->key==NULL) return 0;
        //新的元素放在链表的第一个位置
        np->next=hashtab[hi];
        hashtab[hi]=np;
    }
    //else
      //  free(np->desc);
    np->val=val;
    
    return 1;
}


void hash_cleanup(){
    int i;
    node *np,*t;
    for(i=0;i<HASHSIZE;i++){
        if(hashtab[i]!=NULL){
            np=hashtab[i];
            while(np!=NULL){
                t=np->next;
                free(np->key);
                free(np);
                np=t;
            }
        }
    }
}

/* A pretty useless but good debugging function,
which simply displays the hashtable in (key.value) pairs
*/
void hash_display(){
    int i;
    node *t;
    for(i=0;i<HASHSIZE;i++){
        if(hashtab[i]==NULL)
            printf("()");
        else{
            t=hashtab[i];
            printf("(");
            for(;t!=NULL;t=t->next)
                printf("[%s.%d] ",t->key,t->val);
            printf(")");
        }
    }
}

//顺序遍历hash表中各个节点
node* hash_next()
{
	if(curNode == NULL || curNode->next == NULL)
	{
		int i = 0;
		for(;i<HASHSIZE;i++,curIndex++){
		    if(hashtab[curIndex%HASHSIZE] != NULL)
		    {
		    	curNode = hashtab[curIndex%HASHSIZE];
		    	curIndex++;
		    	break;
		    }
    	}
	}
	else
	{
		curNode = curNode->next;
	}
	curIndex = curIndex%HASHSIZE;
	return curNode;
	
}
