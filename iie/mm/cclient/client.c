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

#define RHOST "127.0.0.1"		//这个参数应该怎么传进来呢。。。
#define RPORT 6379

#define LPORT 11111
#define LHOST "127.0.0.1"

#define SYNCSTORE 1
#define SEARCH 2
#define ASYNCSTORE 4
#define SERVERINFO 5



int connectToRedis();
char* mmsearchPhoto(char* info,int* length);
char* mmgetPhoto(char* set,char* md5,int* length);

void  inttochars(int num,char* chars);
redisContext* rc = NULL;		//redis连接的上下文
int syncStoreSock = -1;
int asyncStoreSock = -1;


//在读取图片前要先连接redis
int connectToRedis()
{
	
	int timeout = 10000;
	struct timeval tv;
	tv.tv_sec = timeout / 1000;
    tv.tv_usec = timeout * 1000;
     //以带有超时的方式链接Redis服务器，同时获取与Redis连接的上下文对象。
     //该对象将用于其后所有与Redis操作的函数。
    rc = redisConnect(RHOST, RPORT);
    if (rc->err) {
         redisFree(rc);
         printf("can't connect to redis at %s:%d\n",RHOST, RPORT);
         return -1;
    }
    return 1;
}

//get photo by set and md5
//return addr of the content, or NULL if any exception occurs.

//(redisReply)When an error occurs, the return value is NULL and the err field in the context will be set (see section on Errors). 
//Once an error is returned the context cannot be reused and you should set up a new connection.
char* mmgetPhoto(char* set,char* md5,int* length)
{
	redisReply* reply = redisCommand(rc,"hget %s %s",set,md5);
	if(reply == NULL)
	{
		printf("read from redis failed:%d",rc->err);
		freeReplyObject(reply);
		redisFree(rc);
        return NULL;
	}
	printf("info,%s\n",reply->str);
	if(reply->type == REDIS_REPLY_NIL)
	{
		printf("%s.%s does not exist on redis server.",set,md5);
		return NULL;
	}
	else
		return mmsearchPhoto(reply->str,length);
}

char* mmsearchPhoto(char* info,int* length)
{
	//解析info
	char *temp = strdup(info);		//strtok函数分割字符串时会改变输入字符串的内容，所以要先复制一份
	//printf("in mm\n");
	char *token=strtok(temp,"#");
	int i = 1;
	char *nodeName = (char*)malloc(20);
	int port;
    while(token!=NULL){
        if(i == 3)
        	nodeName = token;
        if(i == 4)
        	port = atoi(token);
        token=strtok(NULL,"#");
        if(token != NULL)
        	i++;
    }
    //
    if(i != 8)
    {
    	printf("Invalid INFO string, info length is %d\n",i);
    	return NULL;
	}
	
	//建立连接
	int searchSock = socket(AF_INET,SOCK_STREAM,0);
	if(searchSock == -1)
	{
		printf("调用socket函数建立socket描述符出错！\n");
		return NULL;
	}
	
	struct hostent *hptr = gethostbyname(nodeName);		//把主机名转换一下，不能直接用主机名建立连接
	if(hptr == NULL)
	{
		printf("resolve nodename:%s failed!\n",nodeName);
		return NULL;
	}
	struct sockaddr_in dest_addr;
	dest_addr.sin_family=AF_INET;/*hostbyteorder*/
	dest_addr.sin_port=htons(port);/*short,network byte order*/
	//dest_addr.sin_addr.s_addr=inet_addr(nodeName);			//zhaoyang-pc连不上
	dest_addr.sin_addr.s_addr = ((struct in_addr *)(hptr->h_addr))->s_addr;
	bzero(&(dest_addr.sin_zero),8);/*zero the rest of the struct*/
	int cr = connect(searchSock,(struct sockaddr*)&dest_addr,sizeof(struct sockaddr));	
	//printf("nodeName:%s,port:%d\n",nodeName,port);
	if(cr == -1)	
	{
		printf("connect to server %s:%d failed\n",nodeName,port);
		return NULL;
	}
	
	char header[4] = {(char)SEARCH,(char)strlen(info)};
	//printf("strlen:%d,%s\n",strlen(info),info);
	send_bytes(searchSock,header,4);
	send_bytes(searchSock,info,strlen(info));
	
	int count = recv_int(searchSock);		
	if(count < 0)
	{
		return NULL;
	}
	printf("count:%d\n",count);			
	char* buf = (char*)malloc(count);
	*length = count;
	if (count >= 0) {
		recv_bytes(searchSock,buf, count);
		
	} else {
		printf("Internal error in mm server.\n");
		return NULL;
	}
	
	//strdup()会先用maolloc()配置与参数s 字符串相同的空间大小, 
	//然后将参数s 字符串的内容复制到该内存地址, 然后把该地址返回. 该地址最后可以利用free()来释放.
	free(temp);		
	return buf;
}

//param length is the length of content
char* syncStorePhoto(char* set,char* md5,char* content,int length)
{
	if(syncStoreSock == -1)
	{
		syncStoreSock = socket(AF_INET,SOCK_STREAM,0);
		if(syncStoreSock == -1)
		{
			printf("调用socket函数建立socket描述符出错！\n");
			return NULL;
		}
	}
	
	struct sockaddr_in dest_addr;
	dest_addr.sin_family=AF_INET;/*hostbyteorder*/
	dest_addr.sin_port=htons(LPORT);/*short,network byte order*/
	dest_addr.sin_addr.s_addr=inet_addr(LHOST);			
	bzero(&(dest_addr.sin_zero),8);/*zero the rest of the struct*/
	int cr = connect(syncStoreSock,(struct sockaddr*)&dest_addr,sizeof(struct sockaddr));	
	//printf("nodeName:%s,port:%d\n",nodeName,port);
	if(cr == -1)	
	{
		printf("connect to server %s:%d failed\n",LHOST,LPORT);
		return NULL;
	}
	
	//action,set,md5,content的length写过去
	char header[4] = {(char)SYNCSTORE,(char)strlen(set),(char)strlen(md5)};
	send_bytes(syncStoreSock,header,4);
	send_int(syncStoreSock,length);
	//set,md5,content的实际内容写过去
	send_bytes(syncStoreSock,set,strlen(set));
	send_bytes(syncStoreSock,md5,strlen(md5));
	send_bytes(syncStoreSock,content,length);
	
	//接收结果
	int count = recv_int(syncStoreSock);
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
	char* result = (char*)malloc(count);
	if(recv_bytes(syncStoreSock,result,count) == 1)
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

void asyncStorePhoto(char* set,char* md5,char* content,int length)
{
	if(asyncStoreSock == -1)
	{
		asyncStoreSock = socket(AF_INET,SOCK_STREAM,0);
		if(asyncStoreSock == -1)
		{
			printf("调用socket函数建立socket描述符出错！\n");
			return ;
		}
	}
	
	struct sockaddr_in dest_addr;
	dest_addr.sin_family=AF_INET;/*hostbyteorder*/
	dest_addr.sin_port=htons(LPORT);/*short,network byte order*/
	dest_addr.sin_addr.s_addr=inet_addr(LHOST);			
	bzero(&(dest_addr.sin_zero),8);/*zero the rest of the struct*/
	int cr = connect(asyncStoreSock,(struct sockaddr*)&dest_addr,sizeof(struct sockaddr));	
	//printf("nodeName:%s,port:%d\n",nodeName,port);
	if(cr == -1)	
	{
		printf("connect to server %s:%d failed\n",LHOST,LPORT);
		return ;
	}
	
	//action,set,md5,content的length写过去
	char header[4] = {(char)SYNCSTORE,(char)strlen(set),(char)strlen(md5)};
	send_bytes(asyncStoreSock,header,4);
	send_int(asyncStoreSock,length);
	//set,md5,content的实际内容写过去
	send_bytes(asyncStoreSock,set,strlen(set));
	send_bytes(asyncStoreSock,md5,strlen(md5));
	send_bytes(asyncStoreSock,content,length);
	
}

void stest()
{
	FILE *fp = fopen("/home/zhaoyang/photo/psb2.gif","rb");
	//curpos = ftell(stream); /* 得到当前的位置，即偏移量 *///取得当前文件指针位置,可能已经移动了文件指针
   	fseek(fp, 0L, SEEK_END);     /* 从文件尾位置向前移动0L    *///移动到文件的结尾
   	int length = ftell(fp);           //获得文件大小
   	char* content = (char*)malloc(length);
   	fseek(fp, 0, SEEK_SET); /* 回到curpos定位的位置                 */ 
   	fread(content,1,length,fp);
	printf("file length:%d,info returned:%s\n",length,syncStorePhoto("st","2",content,length));
	//asyncStorePhoto("st","2",content,length);
}

void gtest()
{
	//int n = 1;
	//char *p = (char*)malloc(n);
	char info[] = "1#loc#zhaoyang-pc#11111#0#7150496#685257#.";
	
	char a[] = "st",b[] = "2";
	
	int count;
	char* content = mmgetPhoto(a,b,&count);	
	
	//char* content = mmsearchPhoto(info,&count);
	if(content == NULL) return ;
	FILE *fp = fopen("abc","wb");
	fwrite(content,1,count,fp);
	fclose(fp);
}

int main()
{
	if(connectToRedis() == -1)
	{
		return -1;
	}
	//stest();
	gtest();
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
