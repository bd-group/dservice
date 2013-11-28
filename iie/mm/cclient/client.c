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

//编译时要加上-l hiredis


#define SYNCSTORE 1
#define SEARCH 2
#define ASYNCSTORE 4
#define SERVERINFO 5

redisContext* rc = NULL;		//redis连接的上下文

int searchByinfo(char *info,void **buf,size_t* length);
int getPhoto(char* set,char* md5,void **buf,size_t* length);
int searchByInfo(char *info,void **buf,size_t* length);

void inttochars(int num,char* chars);
int countChInStr(char* str,char c);

//get photo by set and md5
//return addr of the content, or NULL if any exception occurs.

//(redisReply)When an error occurs, the return value is NULL and the err field in the context will be set (see section on Errors). 
//Once an error is returned the context cannot be reused and you should set up a new connection.
int getPhoto(char* set,char* md5,void **buf,size_t* length)
{
	redisReply* reply = redisCommand(rc,"hget %s %s",set,md5);
	if(reply == NULL)
	{
		printf("read from redis failed:%d",rc->err);
		freeReplyObject(reply);
		redisFree(rc);
        return -1;
	}
	//printf("in getphoto info,%s\n",reply->str);
	if(reply->type == REDIS_REPLY_NIL)
	{
		printf("%s.%s does not exist on redis server.",set,md5);
		return -1;
	}
	else
		return searchPhoto(reply->str,buf,length);
}


int searchByInfo(char *info,void **buf,size_t* length)
{
	//解析info
	char *temp = strdup(info);		//strtok函数分割字符串时会改变输入字符串的内容，所以要先复制一份
	//printf("in mm\n");
	char *token=strtok(temp,"@");
	int i = 1;
	char *nodename ,*portp;
	int port;
    while(token!=NULL){
    //printf("in searchbyinfo: %s\n",token);
        if(i == 3)
        	nodename = token;
        if(i == 4)
        	portp = token;
        token=strtok(NULL,"@");
        if(token != NULL)
        	i++;
    }
    if(i != 8)
    {
    	printf("Invalid INFO string:%s.\n",info);
    	return -1;
	}
	char *nodeport = (char*)malloc(strlen(nodename)+strlen(portp)+2);
	strcpy(nodeport,nodename);
	strcat(nodeport,":");
	strcat(nodeport,portp);
	int sock = hash_get(nodeport);
	if(sock == -1)
	{
		printf("no connection for established with server:%s\n",nodeport);
		free(nodeport);
		return -1;
	}
	
	char header[4] = {(char)SEARCH,(char)strlen(info)};
	//printf("strlen:%d,%s\n",strlen(info),info);
	send_bytes(sock,header,4);
	send_bytes(sock,info,strlen(info));
	
	int count = recv_int(sock);		
	if(count < 0)
	{
		return -1;
	}
	//printf("in searchbyinfo count:%d\n",count);			
	*buf = (char*)malloc(count);
	//printf("in searchbyinfo *buf :%ld\n",*buf);
	*length = (size_t)count;
	if (count >= 0) {
		recv_bytes(sock,*buf, count);
		
	} else {
		printf("Internal error in mm server.\n");
		return -1;
	}
	
	//strdup()会先用maolloc()配置与参数s 字符串相同的空间大小, 
	//然后将参数s 字符串的内容复制到该内存地址, 然后把该地址返回. 该地址最后可以利用free()来释放.
	free(temp);		
	free(nodeport);
	return 1;
}

//传过来的info可能是包含副本信息的，这里的info必须是char[]类型的
int searchPhoto(char* infos,void **buf,size_t* length)
{
	int n = countChInStr(infos,'#');
	if(n == 0)
		return searchByInfo(infos,buf,length);
	else
	{
		//在searchbyinfo里面调用了strtok来分割字符串,这里不能再用，使用另一种方法分割字符串
		char *p = rindex(infos,'#');
		while(p != 0)
		{
			printf("in searchphoto: %s\n",p);
			int r = searchByInfo(p+1,buf,length);
			if(r == 1)
			{
				return 1;
			}
			else
			{
    			*p = '\0';
				p = rindex(infos, '#');
				continue;
			}
		}	
	}
	
	return searchByInfo(infos,buf,length);
}

//param length is the length of content
char* syncStorePhoto(char* set,char* md5,void* content,size_t length,int sock)
{
	
	//action,set,md5,content的length写过去
	char header[4] = {(char)SYNCSTORE,(char)strlen(set),(char)strlen(md5)};
	send_bytes(sock,header,4);
	send_int(sock,length);
	//set,md5,content的实际内容写过去
	send_bytes(sock,set,strlen(set));
	send_bytes(sock,md5,strlen(md5));
	send_bytes(sock,content,length);
	
	//接收结果
	int count = recv_int(sock);
	if(count == -1)
	{
		redisReply* reply = redisCommand(rc,"hget %s %s",set,md5);
		if(reply == NULL)
		{
			printf("read from redis failed:%d",rc->err);
			freeReplyObject(reply);
			redisFree(rc);
		    return NULL;
		}
		return reply->str;
	}
	printf("sync store:%d\n",count);
	char* result = (char*)malloc(count+1);
	*(result+count) = '\0';		//还得给字符串留个结尾
	if(recv_bytes(sock,result,count) == 1)
	{
		if(result[0] == '#')
		{
			printf("MM server failure: %s",result);
			return NULL;
		}
		return result;
	}
	else
		return NULL;

}

void asyncStorePhoto(char* set,char* md5,char* content,int length,int sock)
{
	
	//action,set,md5,content的length写过去
	char header[4] = {(char)SYNCSTORE,(char)strlen(set),(char)strlen(md5)};
	send_bytes(sock,header,4);
	send_int(sock,length);
	//set,md5,content的实际内容写过去
	send_bytes(sock,set,strlen(set));
	send_bytes(sock,md5,strlen(md5));
	send_bytes(sock,content,length);
	
}

//计算字符串str中包含几个字符c
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

int recv_int(int sockfd) 
{
	char bytes[4];
	memset(bytes,0,sizeof(bytes));
	
	if(recv_bytes(sockfd,bytes,4) == 1)
		return (bytes[0]&0xff)<<24 | (bytes[1]&0xff)<<16 
		| (bytes[2]&0xff)<<8 | bytes[3]&0xff;
	else
	{
		printf("recv int failed\n");
		return -2;
	}

}

void  inttochars(int num,char* chars)
{
	chars[3] = (char)(num & 0xff);
	chars[2] = (char)((num>>8) & 0xff);
	chars[1] = (char)((num>>16) & 0xff);
	chars[0] = (char)((num>>24) & 0xff);
	
}

int send_int(int sockfd,int num)
{
	char bytes[4];
	inttochars(num,bytes);
	return send_bytes(sockfd,bytes,4);
}

int recv_bytes(int sockfd,char* buf,int n)
{
	int i = 0;
	while(i<n)
	{
		int a = recv(sockfd,buf+i,n-i,0);
		if(a == -1)
		{
			printf("recv bytes failed.\n");
			return -1;
		}

		i+=a;
		
		if(a == 0 && i<n)
		{
			printf("input stream end in execption.\n");
			return -1;
		}
		
	}
	return 1;
}
int send_bytes(int sockfd,char* buf,int n)
{
	int i = 0;
	while(i<n)
	{
		int a = send(sockfd,buf+i,n-i,0);
		if(a == -1)
		{
			printf("send bytes failed.\n");
			return -1;
		}

		i += a;
	}
	return 1;
}
