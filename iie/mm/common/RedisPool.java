package iie.mm.common;

import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;

public class RedisPool {
	private MMConf conf;
	private JedisSentinelPool jsp = null;
	private JedisCluster jc = null;
	private String masterName = null;
	private AtomicLong alloced = new AtomicLong(0);
	private AtomicLong balanceTarget = new AtomicLong(0);
	private String pid;
	
	public RedisPool(MMConf conf, String masterName) {
		this.conf = conf;
		this.masterName = masterName;
	}
	
	public void quit() {
		if (jsp != null)
			jsp.destroy();
	}
	
	public static Jedis getRawInstance(String host, int port) {
		return new Jedis(host, port);
	}
	
	public Jedis getResource() throws JedisException {
		switch (conf.getRedisMode()) {
		case SENTINEL: {
			if (jsp != null)
				return jsp.getResource();
			else {
				JedisPoolConfig c = new JedisPoolConfig();
				jsp = new JedisSentinelPool(masterName, conf.getSentinels(), c, 
						conf.getRedisTimeout());
				System.out.println("New pool " + masterName);
				return jsp.getResource();
			}
		}
		case CLUSTER: {
		}
		}
		return null;
	}
	
	public Jedis putInstance(Jedis j) {
		try {
			if (j == null)
				return null;
			switch (conf.getRedisMode()) {
			case SENTINEL:
				jsp.returnResourceObject(j);
			}
		} catch (Exception e) {
			jsp.destroy();
			jsp = null;
		}
		return null;
	}

	public AtomicLong getAlloced() {
		return alloced;
	}

	public void setAlloced(AtomicLong alloced) {
		this.alloced = alloced;
	}

	public AtomicLong getBalanceTarget() {
		return balanceTarget;
	}

	public void setBalanceTarget(AtomicLong balanceTarget) {
		this.balanceTarget = balanceTarget;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}
}
