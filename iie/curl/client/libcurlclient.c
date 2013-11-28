#define CURL_STATICLIB  //必须在包含curl.h前定义

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <curl/easy.h>
#include <hiredis/hiredis.h> 

//以下四项是必须的
#pragma comment ( lib, "libcurl.lib" )
#pragma comment ( lib, "ws2_32.lib" )
#pragma comment ( lib, "winmm.lib" )
#pragma comment ( lib, "wldap32.lib" )

 #define MAX_BUF 	 65536 

char wr_buf[MAX_BUF+1]; 
int  wr_index; 

int init(char *url);
int libcurlget(char *server, char *key, void **buffer, size_t *len);

static redisReply* reply;
      
/************************************************************************/
/* 功能: 初始化,此处的url为服务器的主机名和端口号，形式为主机名：端口号（例 localhost:8080），
		url获取途径是通过解析数据库DB的properties属性，properties属性的key的值是mm.url，
		通过此key的值获取value，即url。
   实现过程：
   		1，connect to redis
		2，得到string数组  保存的httpservice
		3，访问地址
/* 返回值：
/************************************************************************/    
int init(char *url)
{
    //根据url切割到所需要的IP和PORT，用strtok函数分割字符串
    char *token=strtok(url,":");
	int i = 1;
	char *ip,*port;
	int iport;
    while(token!=NULL){
		if(i == 1)
			ip = token;
       	if(i == 2)
        	port = token;
        token=strtok(NULL,":");
        if(token != NULL)
        	i++;
      }
	iport = atoi(port);
	//根据得到的ip和port，连接redis
    int timeout = 10000;  
    struct timeval tv;  
    tv.tv_sec = timeout / 1000;  
    tv.tv_usec = timeout * 1000;  
    //以带有超时的方式链接Redis服务器，同时获取与Redis连接的上下文对象。  
    //该对象将用于其后所有与Redis操作的函数。
    redisContext* c = redisConnect(ip, iport);  
	if (c->err) {  
		redisFree(c);  
		return;  
	} 
	//从redis获得需要的内容
	const char* command = "ZRANGE mm.active.http 0 -1"; 
	reply = (redisReply*)redisCommand(c,command);
	//需要注意的是，如果返回的对象是NULL，则表示客户端和服务器之间出现严重错误，必须重新链接。 
	if (NULL == reply) {  
          redisFree(c);  
         return;  
    }  
    //不同的Redis命令返回的数据类型不同，在获取之前需要先判断它的实际类型。
    //字符串类型的set命令的返回值的类型是REDIS_REPLY_STATUS，REDIS_REPLY_ARRAY命令返回一个数组对象。
	/*
	if (!(reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str,"OK") == 0)) {  
         printf("Failed to execute command[%s].\n",command);  
         freeReplyObject(reply);  
         redisFree(c);  
         return;  
    }  
    */
    
    if ( reply->type == REDIS_REPLY_ERROR )  
        printf( "Error: %s\n", reply->str );  
    else if ( reply->type != REDIS_REPLY_ARRAY )  
        printf( "Unexpected type: %d\n", reply->type );  
    else {  
        for ( i=0; i<reply->elements; ++i ){  
        	printf( "Result:%d: %s\n", i, reply->element[i]->str );  
        }  
    }  
	printf( "Total Number of Results: %d\n", i ); //测试
	printf( "Total Server of Results ( only one ): %s\n", reply->element[0]->str ); 
    printf("Succeed to execute command[%s].\n",command); 

	//由于后面重复使用该变量，所以需要提前释放，否则内存泄漏。  
    //freeReplyObject(reply);  
	
	return 0;
}  
  
/************************************************************************/
/* 功能: 同步的存储一个多媒体对象，并返回其存储元信息
/* 参数: 此处的key是对应多媒体内容的集合和“键”（形如set:md5）形成的字符串，content为多媒体的内容组成的字节数组
/************************************************************************/      
char *put(char *key, void *content, size_t len)
{

}

/************************************************************************/
/* 功能: 异步的存储从redis获取LHOST,LPORT一个多媒体对象，不返回任何信息
/* 参数: 此处的key是对应多媒体内容的集合和“键”（形如set:md5）形成的字符串，content为多媒体的内容组成的字节数组
/************************************************************************/ 
char *iput(char *key, void *content, size_t len)
{

}


/************************************************************************/
/* 功能: 同步批量的存储一个多媒体对象，并返回其存储元信息
/* 参数: 此处的key是对应多媒体内容的集合和“键”（形如set:md5）形成的字符串，content为多媒体的内容组成的字节数组
/************************************************************************/  
char *mput(char **key, void **content, size_t len, int keynr)
{

}

/************************************************************************/
/* 功能: 同步的对单个多媒体对象进行读取，通过接收混合的key（可以是set:key(形如set:md5)，或者是索引信息），
		返回由单个多媒体内容组成的字节数组
/* 参数: 
/************************************************************************/  
int get(char *key, void **buffer, size_t *len)
{

}

/************************************************************************/
/* 功能: 异步的对单个多媒体对象进行读取，通过接收混合的key（可以是set:key(形如set:md5)，
		或者是索引信息），返回一个ID；
/* 参数: 
/************************************************************************/  
long iget(char *key)
{

}


/************************************************************************/
/* 功能: 异步批量的对多个多媒体对象读取，接受由混合的key（可以是set:key(形如set:md5)），
		或者是索引信息组成的字符串数组，返回值为由多个ID组成的set集合；
/* 参数: 
/************************************************************************/  
int imget(char **key, void **buffer, size_t *len, int keynr)
{

}

/************************************************************************/
/* 功能: 将imGet方法返回的由多个ID组成的set集合作为参数接入，等待服务器的处理，处理完将结果写入Map集合，
		此map的key是imGet方法中要取得的多媒体对象的key，value为由服务器返回的与key对应的多媒体对象。
/* 参数: 
/************************************************************************/  
int wait(long *ids, int nr, void **buffer, size_t *len)
{

}

/************************************************************************/
/* 功能: 核心功能
/* 参数: server key
/************************************************************************/ 
    
/* This can use to download images according it's url.   */
      
size_t write_data_test(void *buffer, size_t size, size_t nmemb, FILE *stream) 
{  
    size_t written;  
    written = fwrite(buffer, size, nmemb, stream);  
    return written;  
}

/*
static void *myrealloc(void *ptr, size_t size)
{
	if(ptr)
   	 	return realloc(ptr, size);
	else
    	return malloc(size);
} 
static size_t write_buffer(void *ptr, size_t size, size_t nmemb, void *data)
{
    size_t realsize = size * nmemb;
  	mem->memory = (char *)myrealloc(mem->memory, mem->size + realsize + 1);
  	if (mem->memory) 
  	{
    memcpy(&(mem->memory[mem->size]), ptr, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
  	}
  	return realsize;
}
*/

/* Write data callback function*/ 

size_t write_data( void *buffer, size_t size, size_t nmemb, void *userp ) 
{ 
	int segsize = size * nmemb; 
	/* Check to see if this data exceeds the size of our buffer.*/ 
	if ( wr_index + segsize > MAX_BUF ) 
	{ 
		*(int *)userp = 1; 
		return 0; 
	} 
	/* Copy the data from the curl buffer into our buffer */ 
	memcpy( (void *)&wr_buf[wr_index], buffer, (size_t)segsize ); 
	/* Update the write index */ 
	wr_index += segsize; 
	/* Null terminate the buffer */ 
	wr_buf[wr_index] = 0; 
	/* Return the number of bytes received, indicating to curl that all is okay */ 
	return segsize; 
} 

int libcurlget(char *server,char *key, void **buffer, size_t *len)
{
    CURL *curl;
    CURLcode res;
    
	int wr_error; 
	wr_error = 0; 
	wr_index = 0; 
	
    /* 初始化libcurl */
    CURLcode code;
    char *error = "error";
    curl = curl_easy_init();
    if (!curl) 
    { 
    	printf("couldn't init curl\n"); 
    	return 0; 
	} 
    if (curl == NULL)
    {
        printf( "Failed to create curl connection\n");
        return -1;
    }    
    if(curl) 
    {    
		char url[4096];
		char str[4096000];
	
		sprintf(url, "http://%s/get?key=%s", server, key);
    
    	curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);  
    	code = curl_easy_setopt(curl, CURLOPT_URL, url);
   		if (code != CURLE_OK)
    	{
        	printf("Failed to set URL [%s]\n", error);
        	return -1;
   	    }
    	code = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
    	if (code != CURLE_OK)
   		{
        	printf( "Failed to set redirect option [%s]\n", error );
        	return -1;
    	}
    	
    	/* 核心功能实现 */
    	/* Tell curl the URL of the file we're going to retrieve */ 
    	curl_easy_setopt(curl, CURLOPT_URL, url);
		/* Tell curl that we'll receive data to the function write_data, and also provide it with a context pointer for our error return. */ 
		curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void *)&wr_error ); 
		curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, write_data );  
		        	
		//curl_easy_setopt(curl, CURLOPT_URL, filename); //设置下载地址
   	 	//curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3);//设置超时时间
    	//curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);//设置写数据的函数
    	//curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_buffer);//设置写数据的函数
    	
    	//curl_easy_setopt(curl, CURLOPT_WRITEDATA, str);//设置写数据的变量
    
    	/* 执行下载 */
		res = curl_easy_perform(curl);
		/* 输出 */
		printf( "res = %d (write_error = %d)\n", res, wr_error ); 
		if ( res == 0 ) printf( "%s\n", wr_buf ); 
   		/* 清理内存，释放curl资源 */
   		curl_easy_cleanup(curl);
   		
    }	
    
    return 0;     
    
}

int main(void)
{
	char url[] = "192.168.1.221:6379";
	init(url);

	void *buffer;
	size_t len;
	int i = 0;
	char key[] = "default@206dd46198a06e912e34c9793afb9ce3	";
	for( i=0; i< reply->elements;i++)
		libcurlget(reply->element[i]->str,key,&buffer,&len);

}