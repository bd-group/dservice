package iie.mm.client;

import iie.mm.client.ClientConf.RedisInstance;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

public class RedisFactory {
	// for each ClientAPI, there should be one redisfactory
	private ClientConf conf;
	private JedisSentinelPool jsp = null;

	public RedisFactory(ClientConf conf) {
		this.conf = conf;
	}
	
	public void quit() {
		if (jsp != null)
			jsp.destroy();
	}
	
	public static Jedis getRawInstance(String host, int port) {
		Jedis jedis = new Jedis(host,port);
		return jedis;
	}
	
	public Jedis getNewInstance(RedisInstance ri) throws JedisConnectionException {
		switch (conf.getRedisMode()) {
		case STANDALONE:
			return new Jedis(ri.hostname, ri.port);
		case SENTINEL:
		{
			Config c = new Config();
			if (jsp != null)
				return jsp.getResource();
			else {
				jsp = new JedisSentinelPool("mymaster", conf.getSentinels(), c, conf.getRedisTimeout());
				return jsp.getResource();
			}
		}
		}
		return null;
	}
	
	public Jedis putInstance(Jedis j) {
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

	public Jedis putBrokenInstance(Jedis j) {
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
