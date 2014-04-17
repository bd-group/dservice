package iie.mm.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

public class ClientConf {
	public static class RedisInstance {
		public String hostname;
		public int port;
		
		public RedisInstance(String hostname, int port) {
			this.hostname = hostname;
			this.port = port;
		}
	}
	private Set<String> sentinels;
	private List<RedisInstance> redisIns;
	private String serverName;
	private int serverPort;
	private int dupNum;			//一个文件存储份数
	private int sockPerServer;
	private boolean autoConf = false;
	private boolean logDupInfo = true;
	
	public static enum MODE {
		DEDUP, NODEDUP,
	};
	
	public enum RedisMode {
		SENTINEL, STANDALONE,
	};
	
	private MODE mode;
	private RedisMode redisMode;
	
	private boolean getkeys_do_sort = true;
	
	public ClientConf(String serverName, int serverPort, String redisHost, int redisPort, MODE mode, 
			int dupNum) throws UnknownHostException {
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
		this.setRedisMode(RedisMode.STANDALONE);
	}
	
	public ClientConf(Set<String> sentinels, MODE mode, int dupNum) throws UnknownHostException {
		redisIns = new ArrayList<RedisInstance>();
		this.dupNum = dupNum;
		this.mode = mode;
		this.setSockPerServer(5);
		this.setSentinels(sentinels);
		this.setRedisMode(RedisMode.SENTINEL);
	}
	
	public ClientConf() {
		redisIns = new ArrayList<RedisInstance>();
		this.dupNum = 1;
		this.mode = MODE.NODEDUP;
		this.setRedisMode(RedisMode.STANDALONE);
		this.setSockPerServer(5);
		this.setAutoConf(true);
	}

	public RedisInstance getRedisInstance() {
		if (redisIns.size() > 0) {
			Random r = new Random();
			return redisIns.get(r.nextInt(redisIns.size()));
		} else 
			return null;
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

	public RedisMode getRedisMode() {
		return redisMode;
	}

	public void setRedisMode(RedisMode redisMode) {
		this.redisMode = redisMode;
	}

	public Set<String> getSentinels() {
		return sentinels;
	}

	public void setSentinels(Set<String> sentinels) {
		this.sentinels = sentinels;
	}

	public boolean isAutoConf() {
		return autoConf;
	}

	public void setAutoConf(boolean autoConf) {
		this.autoConf = autoConf;
	}

	public boolean isGetkeys_do_sort() {
		return getkeys_do_sort;
	}

	public void setGetkeys_do_sort(boolean getkeys_do_sort) {
		this.getkeys_do_sort = getkeys_do_sort;
	}

	public boolean isLogDupInfo() {
		return logDupInfo;
	}

	public void setLogDupInfo(boolean logDupInfo) {
		this.logDupInfo = logDupInfo;
	}
}
