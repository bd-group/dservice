package iie.mm.server;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * 代表节点的配置
 * 
 * @author zhaoyang
 * 
 */
public class ServerConf {
	public static int DEFAULT_SERVER_PORT = 30303;
	public static int DEFAULT_REDIS_PORT = 30308;
	public static int DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
	public static int DEFAULT_PERIOD = 10;
	public static int DEFAULT_FLUSH_INTERVAL = 10;
	public static int DEFAULT_REQNR_TO_FLUSH = 15;
	public static int DEFAULT_HTTP_PORT = 20202;
	
	private boolean use_junixsocket = false;
	
	private String nodeName; // 节点名
	private int serverPort = DEFAULT_SERVER_PORT;
	private String redisHost;
	private int redisPort = DEFAULT_REDIS_PORT;
	private int blockSize = DEFAULT_BLOCK_SIZE;
	private int httpPort = DEFAULT_HTTP_PORT;
	private int period = DEFAULT_PERIOD; // 每隔period秒统计一次读写速率
	
	private int flush_interval = DEFAULT_FLUSH_INTERVAL;
	private int reqnr_to_flush = DEFAULT_REQNR_TO_FLUSH;
	
	private Set<String> storeArray = new HashSet<String>();

	public ServerConf(String nodeName, int serverPort, String redisHost, int redisPort, int blockSize, int period, int httpPort) throws Exception {
		if (nodeName == null)
			this.nodeName = InetAddress.getLocalHost().getHostName();
		if (serverPort > 0)
			this.serverPort = serverPort;
		if (redisHost == null)
			this.redisHost = this.nodeName;
		else
			this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.blockSize = blockSize;
		this.period = period;
		this.httpPort = httpPort;
		
		// ok, get global config if they exist.
		Jedis jedis = new RedisFactory(this).getDefaultInstance();
		Pipeline p = jedis.pipelined();
		p.get("mm.conf.blocksize");
		p.get("mm.conf.period");
		List<Object> results = p.syncAndReturnAll();
		if (results.get(0) != null) {
			this.blockSize = Integer.parseInt(results.get(1).toString());
			System.out.println("Get blockSize from redis server: " + this.blockSize);
		}
		if (results.get(1) != null) {
			this.period = Integer.parseInt(results.get(2).toString());
			System.out.println("Get period from redis server: " + this.period);
		}
		
	}


	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String name) {
		this.nodeName = name;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public String getRedisHost() {
		return redisHost;
	}

	public void setRedisHost(String redisHost) {
		this.redisHost = redisHost;
	}

	public int getRedisPort() {
		return redisPort;
	}

	public void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}

	public int getHttpPort(){
		return httpPort;
	}
	
	public void setHttpPort(int httpPort){
		this.httpPort = httpPort;
	}
	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public int getPeriod() {
		return period;
	}

	public void setPeriod(int period) {
		this.period = period;
	}


	public int getFlush_interval() {
		return flush_interval;
	}


	public void setFlush_interval(int flush_interval) {
		this.flush_interval = flush_interval;
	}


	public int getReqnr_to_flush() {
		return reqnr_to_flush;
	}


	public void setReqnr_to_flush(int reqnr_to_flush) {
		this.reqnr_to_flush = reqnr_to_flush;
	}


	public boolean isUse_junixsocket() {
		return use_junixsocket;
	}


	public void setUse_junixsocket(boolean use_junixsocket) {
		this.use_junixsocket = use_junixsocket;
	}


	public Set<String> getStoreArray() {
		return storeArray;
	}


	public void setStoreArray(Set<String> storeArray) {
		this.storeArray = storeArray;
	}

}
