#include "hashtable.h"
#include "clientapi.h"
#include<sys/types.h>
#include<sys/socket.h>
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include <arpa/inet.h>
#include <netinet/in.h> 
#include <hiredis/hiredis.h> 
#include <netdb.h>
#include <unistd.h>


#define DUPNUM 2

extern redisContext* rc;

//连接服务端，如果成功返回socket描述符，出错返回-1
//有很长的超时时间，如果哪个服务器没启动就会等很久．．
static int connectToServer(char *nodename,int port)
{
	int sock = socket(AF_INET,SOCK_STREAM,0);
	if(sock == -1)
	{
		printf("failed to create a socket！\n");
		return -1;
	}
	
	/*
	struct timeval timeo;
    socklen_t len = sizeof(timeo);
    timeo.tv_sec = 10;
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeo, len) == -1)
    {
        perror("setsockopt");
       	return 0;
    }
	*/
	struct hostent *hptr = gethostbyname(nodename);		//把主机名转换一下，不能直接用主机名建立连接
	if(hptr == NULL)
	{
		printf("resolve nodename:%s failed!\n",nodename);
		return -1;
	}
	struct sockaddr_in dest_addr;
	dest_addr.sin_family=AF_INET;/*hostbyteorder*/
	dest_addr.sin_port=htons(port);/*short,network byte order*/
	//dest_addr.sin_addr.s_addr=inet_addr(nodeName);			//zhaoyang-pc连不上
	dest_addr.sin_addr.s_addr = ((struct in_addr *)(hptr->h_addr))->s_addr;
	bzero(&(dest_addr.sin_zero),8);/*zero the rest of the struct*/
	int cr = connect(sock,(struct sockaddr*)&dest_addr,sizeof(struct sockaddr));	
	//printf("nodeName:%s,port:%d\n",nodeName,port);
	if(cr == -1)	
	{
		printf("connect to server %s:%d failed\n",nodename,port);
		return -1;
	}
	
	return sock;
}

//uris必须传char[] ,不能是char*，好像是因为strtok会改变传入的字符串
int init(char *uris)
{
	hash_init();

	char *token=strtok(uris,":");
	int i = 1;
	char *rh,*rportp;
	int rp;
    while(token!=NULL){
    	//printf("%s\n",token);
        if(i == 1)
        	rh = token;
        if(i == 2)
        	rportp = token;
        token=strtok(NULL,":");
        if(token != NULL)
        	i++;
    }
    
    if(i != 2)
    {
    	printf("wrong format of uris.\n");
    	return -1;
	}
	rp = atoi(rportp);
	
	//连接redis
	int timeout = 10000;
	struct timeval tv;
	tv.tv_sec = timeout / 1000;
    tv.tv_usec = timeout * 1000;
     //以带有超时的方式链接Redis服务器，同时获取与Redis连接的上下文对象。
     //该对象将用于其后所有与Redis操作的函数。
    //rc 是在client.c里面声明的
    rc = redisConnect(rh, rp);
    if (rc->err) {
         redisFree(rc);
         printf("can't connect to redis at %s:%d\n",rh, rp);
         return -1;
    }
    
    redisReply* reply = redisCommand(rc,"smembers mm.active");
    if(reply == NULL)
	{
		printf("read from redis failed:%d",rc->err);
		freeReplyObject(reply);
		redisFree(rc);
        return -1;
	}
	for(i = 0;i	< reply->elements;i++)
	{
		
		char *temp = strdup(reply->element[i]->str);
		char *nodename = strtok(temp,":");
		int port = atoi(strtok(NULL,":"));
		int sock = connectToServer(nodename,port);
		if(sock == -1)
			continue;
		printf("connect to %s success.\n",reply->element[i]->str);
		hash_put(reply->element[i]->str,sock);
		free(temp);
	}
	
    freeReplyObject(reply);
    
    return 0;
}

char *put(char *key, void *content, size_t len)
{
	char *set,*md5;
	int i = 1;
	char *token = strtok(key,"@");
	while(token != NULL)
	{
		if(i == 1)
			set = token;
		if(i == 2)
			md5 = token;
		token=strtok(NULL,"@");
        if(token != NULL)
        	i++;
	}
	if(i != 2)
	{
		printf("wrong format of key:%s@%s",set,md5);
		return NULL;
	}
	char *r = NULL;
	for(i = 0;i<DUPNUM;i++)
	{
		int sock = hash_next()->val;
		if(r != NULL)		//得到多个回应的话，要把之前一个free掉
			free(r);
		r = syncStorePhoto(set,md5,content,len,sock);
	}
	return r;
}

int get(char *key, void **buffer, size_t *len)
{
	int n = countChInStr(key,'@') + countChInStr(key,'#');
	//printf("in get n:%d\n",n);
	if(n == 1)
	{
		char *set = strtok(key,"@");
		char *md5 = strtok(NULL,"@");
		return getPhoto(set,md5,buffer,len);
	}
	else if((n+1)%8 == 0)
	{
		return searchPhoto(key,buffer,len);
	}
	else
	{
		printf("wrong format of key:%s\n",key);
		return -1;
	}
}


