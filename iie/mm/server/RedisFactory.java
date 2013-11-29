package iie.mm.server;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
public class RedisFactory {

//	private static ServerConf conf;
	private static JedisSentinelPool jsp = null;
	public RedisFactory() {
		
	}
	public static void init(ServerConf conf){
		jsp = new JedisSentinelPool(conf.getRedisMasterName(),conf.getStn());
	}
	
	// 从配置文件中读取redis的地址和端口,以此创建jedis对象
	public static synchronized Jedis getResource() {
		return jsp.getResource();
	}
	public static synchronized void returnResource(Jedis jedis){
		jsp.returnResource(jedis);
	}
//	public static ServerConf getServerConf(){
//		return this.conf;
//	}
//	
//	public static void setServerConf(ServerConf conf){
//		this.conf = conf;
//	}
}
