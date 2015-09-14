package iie.mm.common;

import java.util.Set;

public class MMConf {
	public enum RedisMode {
		STANDALONE, SENTINEL, CLUSTER,
	};

	private Set<String> sentinels;
	
	private RedisMode redisMode;
	
	private int redisTimeout = 30 * 1000;

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

	public int getRedisTimeout() {
		return redisTimeout;
	}

	public void setRedisTimeout(int redisTimeout) {
		this.redisTimeout = redisTimeout;
	}
}
