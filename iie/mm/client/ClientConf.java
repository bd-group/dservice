package iie.mm.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class ClientConf {
	public static class RedisInstance {
		public String hostname;
		public int port;
		
		public RedisInstance(String hostname, int port) {
			this.hostname = hostname;
			this.port = port;
		}
	}
	private List<RedisInstance> redisIns;
	private String serverName;
	private int serverPort;
	private int dupNum;			//一个文件存储份数
	private int sockPerServer;
	public static enum MODE {
		DEDUP, NODEDUP,
	};
	private MODE mode;
	
	public ClientConf(String serverName, int serverPort, String redisHost, int redisPort, MODE mode,int dupNum) throws UnknownHostException {
		if (serverName != null)
			this.setServerName(serverName);
		else
			this.setServerName(InetAddress.getLocalHost().getHostName());
		this.setServerPort(serverPort);
		if (redisHost == null) {
			throw new UnknownHostException("Invalid redis server host name.");
		}
		redisIns = new ArrayList<RedisInstance>();
		redisIns.add(new RedisInstance(redisHost, redisPort));
		this.mode = mode;
		
		this.dupNum = dupNum;
		this.setSockPerServer(5);
	}
	
	public ClientConf() {
		redisIns = new ArrayList<RedisInstance>();
		this.dupNum = 1;
		this.mode = MODE.NODEDUP;
		this.setSockPerServer(5);
	}

	public RedisInstance getRedisInstance() {
		Random r = new Random();
		return redisIns.get(r.nextInt(redisIns.size()));
	}
	
	public void setRedisInstance(RedisInstance ri) {
		redisIns.add(ri);
	}
	
	public void clrRedisIns() {
		redisIns.clear();
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public MODE getMode() {
		return mode;
	}

	public void setMode(MODE mode) {
		this.mode = mode;
	}
	
	public int getDupNum(){
		return this.dupNum;
	}
	
	public void setDupNum(int dupNum){
		this.dupNum = dupNum;
	}

	public int getSockPerServer() {
		return sockPerServer;
	}

	public void setSockPerServer(int sockPerServer) {
		this.sockPerServer = sockPerServer;
	}
}
