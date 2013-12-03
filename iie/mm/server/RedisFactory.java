package iie.mm.server;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
<<<<<<< HEAD
public class RedisFactory {

//	private static ServerConf conf;
	private static JedisSentinelPool jsp = null;
	public RedisFactory() {
		
=======

public class RedisFactory {
	private static ServerConf conf;
	private static JedisSentinelPool jsp = null;

	public RedisFactory(ServerConf conf) {
		RedisFactory.conf = conf;
>>>>>>> origin/master
	}
	public static void init(ServerConf conf){
		jsp = new JedisSentinelPool(conf.getRedisMasterName(),conf.getStn());
	}
	
	// 从配置文件中读取redis的地址和端口,以此创建jedis对象
<<<<<<< HEAD
	public static synchronized Jedis getResource() {
		return jsp.getResource();
	}
	public static synchronized void returnResource(Jedis jedis){
		jsp.returnResource(jedis);
=======
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
>>>>>>> origin/master
	}
//	public static ServerConf getServerConf(){
//		return this.conf;
//	}
//	
//	public static void setServerConf(ServerConf conf){
//		this.conf = conf;
//	}
}
