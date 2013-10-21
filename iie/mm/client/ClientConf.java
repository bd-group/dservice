package iie.mm.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ClientConf {
	private String redisHost;
	private int redisPort;
	private String localHostName; 
	
	public ClientConf(String localHostName, String redisHost, int redisPort) throws UnknownHostException {
		if (localHostName != null)
			this.setLocalHostName(localHostName);
		else
			this.setLocalHostName(InetAddress.getLocalHost().getHostName());
		if (redisHost == null) {
			throw new UnknownHostException("Invalid redis server host name.");
		}
		this.redisHost = redisHost;
		this.redisPort = redisPort;
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

	public String getLocalHostName() {
		return localHostName;
	}

	public void setLocalHostName(String localHostName) {
		this.localHostName = localHostName;
	}
	
}
