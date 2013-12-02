package iie.mm.server;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

public class RedisFactory {
	private static ServerConf conf;
	private static JedisSentinelPool jsp = null;

	public RedisFactory(ServerConf conf) {
		RedisFactory.conf = conf;
	}

	// 从配置文件中读取redis的地址和端口,以此创建jedis对象
	public Jedis getDefaultInstance() {
		switch (conf.getRedisMode()) {
		case STANDALONE:
			return new Jedis(conf.getRedisHost(), conf.getRedisPort());
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
