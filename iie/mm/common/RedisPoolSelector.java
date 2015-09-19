package iie.mm.common;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class RedisPoolSelector {
	// Note-XXX: in STANDALONE mode, L2 pool is the same as L1 pool
	public class RedisConnection {
		public Jedis jedis;
		public RedisPool rp;
		// L1 or L2.x
		public String id;
	}
	public enum ALLOC_POLICY {
		ROUND_ROBIN, LOAD_BALANCE,
	}
	private ALLOC_POLICY type = ALLOC_POLICY.ROUND_ROBIN;
	private int lastIdx = 0;
	private MMConf conf;
	private RedisPool rpL1;
	private ConcurrentHashMap<String, RedisPool> rpL2 = 
			new ConcurrentHashMap<String, RedisPool>();
	private TimeLimitedCacheMap cached = 
			new TimeLimitedCacheMap(270, 60, 300, TimeUnit.SECONDS);
	
	public RedisPoolSelector(MMConf conf, RedisPool rpL1) throws Exception {
		this.conf = conf;
		this.rpL1 = rpL1;
		Jedis jedis = rpL1.getResource();
		if (jedis == null)
			throw new Exception("get RedisPool from L1 failed: get null");

		switch (conf.getRedisMode()) {
		case SENTINEL:
			try {
				// connect to L1, get all L2 servers to construct a L2 pool
				Map<String, String> l2mn = jedis.hgetAll("mm.l2");

				for (Map.Entry<String, String> entry : l2mn.entrySet()) {
					// get master redis instance from sentinel by masterName
					RedisPool rp = new RedisPool(conf, entry.getValue());
					rp.setPid(entry.getValue() + "." + entry.getKey());
					rpL2.putIfAbsent(entry.getKey(), rp);
				}
				// get client alloc policy
				String stype = jedis.get("mm.alloc.policy");
				if (stype == null || stype.equalsIgnoreCase("round_robin"))
					type = ALLOC_POLICY.ROUND_ROBIN;
				else if (stype.equalsIgnoreCase("load_balance"))
					type = ALLOC_POLICY.LOAD_BALANCE;
			} catch (JedisException je) {
				je.printStackTrace();
			} finally {
				rpL1.putInstance(jedis);
			}
			break;
		case STANDALONE:
			if (rpL1 != null)
				rpL2.putIfAbsent("0", rpL1);
			break;
		case CLUSTER:
			break;
		}
	}
	
	public void setRpL1(RedisPool rpL1) {
		this.rpL1 = rpL1;
	}
	
	public String lookupSet(String set) throws Exception {
		return __lookupSet(set);
	}
	
	private String __lookupSet(String set) throws Exception {
		String id = null;
		Jedis jedis = rpL1.getResource();
		if (jedis == null) 
			throw new Exception("lookup from L1 faield: get null");
		
		try {
			id = jedis.get("`" + set);
			if (id != null) {
				cached.put(set, id);
			}
		} finally {
			rpL1.putInstance(jedis);
		}
		
		return id;
	}
	
	private String __createSet(String set) throws Exception {
		String id = null;
		Jedis jedis = rpL1.getResource();
		if (jedis == null)
			throw new Exception("lookup from L1 failed: get null");
		
		try {
			switch (type) {
			default:
			case ROUND_ROBIN:
				ArrayList<String> ids = new ArrayList<String>();
				ids.addAll(rpL2.keySet());
				if (lastIdx >= ids.size())
					lastIdx = 0;
				if (ids.size() > 0)
					id = ids.get(lastIdx);
				break;
			case LOAD_BALANCE:
				Long min = Long.MAX_VALUE;
				for (Map.Entry<String, RedisPool> entry : rpL2.entrySet()) {
					if (entry.getValue().getBalanceTarget().get() < min) {
						min = entry.getValue().getBalanceTarget().get();
						id = entry.getKey();
					}
				}
				break;
			}
			if (id != null) {
				jedis.setnx("`" + set, id);
				id = jedis.get("`" + set);
				rpL2.get(id).incrAlloced();
			}
		} finally {
			rpL1.putInstance(jedis);
		}
		
		return id;
	}
	
	private RedisPool __lookupL2(String id) throws Exception {
		Jedis jedis = rpL1.getResource();
		if (jedis == null)
			throw new Exception("lookup from L1 failed: get null");
		
		try {
			String masterName = jedis.hget("mm.l2", id);
			if (masterName != null) {
				RedisPool rp = new RedisPool(conf, masterName);
				rp.setPid(masterName + "." + id);
				rpL2.putIfAbsent(id, rp);
			}
		} finally {
			rpL1.putInstance(jedis);
		}
		return rpL2.get(id);
	}
	
	private RedisConnection __getL2_standalone(String set, boolean doCreate) {
		RedisConnection rc = new RedisConnection();
		rc.rp = rpL2.get("0");
		rc.id = "STA:0";
		if (rc.rp != null) {
			rc.jedis = rc.rp.getResource();
		}
		return rc;
	}
	
	private RedisConnection __getL2_sentinel(String set, boolean doCreate) 
			throws Exception {
		RedisConnection rc = new RedisConnection();
		String id = (String)cached.get(set);
		
		if (id == null) {
			// lookup from L1
			id = __lookupSet(set);
		}
		if (id == null && doCreate) {
			id = __createSet(set);
		}
		if (id != null) {
			RedisPool rp = rpL2.get(id);
			if (rp == null) {
				// lookup from L1 by id
				rp = __lookupL2(id);
			}
			rc.rp = rp;
			rc.id = "L2." + id;
		}
		if (rc.rp != null) {
			rc.jedis = rc.rp.getResource();
		}

		return rc;
	}
	
	public RedisConnection getL2(String set, boolean doCreate) throws Exception {
		switch (conf.getRedisMode()) {
		case SENTINEL:
			return __getL2_sentinel(set, doCreate);
		case STANDALONE:
			return __getL2_standalone(set, doCreate);
		case CLUSTER:
		}
		return null;
	}
	
	public void putL2(RedisConnection rc) {
		if (rc != null && rc.jedis != null)
			rc.rp.putInstance(rc.jedis);
	}
	
	public void quit() {
		for (Map.Entry<String, RedisPool> entry : rpL2.entrySet()) {
			entry.getValue().quit();
		}
		rpL2.clear();
	}
	
	public ConcurrentHashMap<String, RedisPool> getRpL2() {
		return rpL2;
	}
}
