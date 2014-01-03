package iie.databak;

import java.util.List;
import java.util.Random;
import java.util.Set;

public class DatabakConf {
	public static class RedisInstance {
		public String hostname;
		public int port;
		
		public RedisInstance(String hostname, int port) {
			this.hostname = hostname;
			this.port = port;
		}
	}
	
	public enum RedisMode {
		SENTINEL, STANDALONE,
	};
	
	private Set<String> sentinels;
	private List<RedisInstance> redisIns;
	private RedisMode redisMode;
	private String zkaddr;
	private String mshost;
	private int msport;
	
	public DatabakConf(Set<String> sentinels, RedisMode redisMode, String zkaddr, String mshost, int msport)
	{
		this.sentinels = sentinels;
		this.redisMode = redisMode;
		this.zkaddr = zkaddr;
		this.mshost = mshost;
		this.msport = msport;
	}
	
	public DatabakConf(List<RedisInstance> redisIns, RedisMode redisMode, String zkaddr, String mshost, int msport)
	{
		this.redisIns = redisIns;
		this.redisMode = redisMode;
		this.zkaddr = zkaddr;
		this.mshost = mshost;
		this.msport = msport;
	}

	public RedisInstance getRedisInstance() {
		if (redisIns.size() > 0) {
			Random r = new Random();
			return redisIns.get(r.nextInt(redisIns.size()));
		} else 
			return null;
	}
	public void setRedisInstance(RedisInstance ri) {
		this.redisIns.add(ri);
	}

	public Set<String> getSentinels() {
		return sentinels;
	}

	public void setSentinels(Set<String> sentinels) {
		this.sentinels = sentinels;
	}

	public RedisMode getRedisMode() {
		return redisMode;
	}

	public void setRedisMode(RedisMode redisMode) {
		this.redisMode = redisMode;
	}

	public String getZkaddr() {
		return zkaddr;
	}

	public void setZkaddr(String zkaddr) {
		this.zkaddr = zkaddr;
	}

	public String getMshost() {
		return mshost;
	}

	public void setMshost(String mshost) {
		this.mshost = mshost;
	}

	public int getMsport() {
		return msport;
	}

	public void setMsport(int msport) {
		this.msport = msport;
	}
	
	
}
