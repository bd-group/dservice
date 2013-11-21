package iie.mm.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ClientConf {
	private String redisHost;
	private int redisPort;
	private String serverName;
	private int serverPort;
	private int dupNum;			//一个文件存储份数
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
		this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.mode = mode;
		
		this.dupNum = dupNum;
	}
	
	public ClientConf() {
		this.dupNum = 1;
		this.mode = MODE.NODEDUP;
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
}
