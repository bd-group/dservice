package iie.mm.client;

import iie.mm.client.ClientConf.RedisInstance;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

public class RedisFactory {
	//专门用来操作redis中数据库0,它里面存储的是md5与存储时的返回值的映射
	private static ClientConf conf;
	private static JedisSentinelPool jsp = null;

	public static void init(ClientConf conf) {
		RedisFactory.conf = conf;
	}
	
	public static void quit() {
		if (jsp != null)
			jsp.destroy();
	}
	
	public static Jedis getRawInstance(String host, int port) {
		Jedis jedis = new Jedis(host,port);
		return jedis;
	}
	
	public static Jedis getNewInstance(RedisInstance ri) {
		switch (conf.getRedisMode()) {
		case STANDALONE:
			return new Jedis(ri.hostname, ri.port);
		case SENTINEL:
		{
			if (jsp != null)
				return jsp.getResource();
			else {
				jsp = new JedisSentinelPool("mymaster", conf.getSentinels());
				return jsp.getResource();
			}
		}
		}
		return null;
	}
	
	public static Jedis putInstance(Jedis j) {
		if (j == null)
			return null;
		switch (conf.getRedisMode()) {
		case STANDALONE:
			break;
		case SENTINEL:
			jsp.returnResource(j);
		}
		return null;
	}

	public static Jedis putBrokenInstance(Jedis j) {
		if (j == null)
			return null;
		switch (conf.getRedisMode()) {
		case STANDALONE:
			break;
		case SENTINEL:
			jsp.returnBrokenResource(j);
		}
		return null;
	}
}
