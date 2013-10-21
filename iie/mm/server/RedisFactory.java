package iie.mm.server;

import redis.clients.jedis.Jedis;

public class RedisFactory {

	private String remoteRedisHost;
	private int remoteRedisPort;

	public RedisFactory(ServerConf conf) {
		remoteRedisHost = conf.getRedisHost();
		remoteRedisPort = conf.getRedisPort();
	}

	// 从配置文件中读取redis的地址和端口,以此创建jedis对象
	public Jedis getDefaultInstance() {
		return new Jedis(remoteRedisHost, remoteRedisPort);
	}

	public static Jedis getNewInstance(String host, int port) {
		Jedis jedis = new Jedis(host, port);
		return jedis;
	}
}
