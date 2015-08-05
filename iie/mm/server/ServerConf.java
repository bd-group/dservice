package iie.mm.server;

import iie.mm.client.Feature.FeatureType;
import iie.mm.client.Feature.FeatureTypeString;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

/**
 * 代表节点的配置
 * 
 * @author zhaoyang
 * 
 */
public class ServerConf {
	public static int DEFAULT_SERVER_PORT = 30303;
	public static int DEFAULT_REDIS_PORT = 30308;
	public static int DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
	public static int DEFAULT_PERIOD = 10;
	public static int DEFAULT_FLUSH_INTERVAL = 10;
	public static int DEFAULT_REQNR_TO_FLUSH = 15;
	public static int DEFAULT_HTTP_PORT = 20202;
	public static int DEFAULT_SYSINFOSTAT_PORT = 19888;
	public static long DEFAULT_WRITE_FD_RECYCLE_TO = 4 * 3600 * 1000;
	public static long DEFAULT_READ_FD_RECYCLE_TO = 2 * 3600 * 1000;
	public static int DEFAULT_RECV_BUFFER_SIZE = 11 * 1024 * 1024;
	public static int DEFAULT_SEND_BUFFER_SIZE = 11 * 1024 * 1024;
	
	private boolean use_junixsocket = false;
	private int verbose = 0;
	
	private String nodeName; // 节点名
	private int serverPort = DEFAULT_SERVER_PORT;
	private String redisHost;
    private int redisPort = DEFAULT_REDIS_PORT;
	private int blockSize = DEFAULT_BLOCK_SIZE;
	private int httpPort = DEFAULT_HTTP_PORT;
	private int period = DEFAULT_PERIOD; // 每隔period秒统计一次读写速率
	
	private int flush_interval = DEFAULT_FLUSH_INTERVAL;
	private int reqnr_to_flush = DEFAULT_REQNR_TO_FLUSH;
	
	private long write_fd_recycle_to = DEFAULT_WRITE_FD_RECYCLE_TO;
	private long read_fd_recycle_to = DEFAULT_READ_FD_RECYCLE_TO;
	
	private static int recv_buffer_size = DEFAULT_RECV_BUFFER_SIZE;
	private static int send_buffer_size = DEFAULT_SEND_BUFFER_SIZE;
	
	private String faceDetectorXML = null;
	private String featureIndexPath = null;
	private static List<FeatureType> features = new ArrayList<FeatureType>();
	
	public static long serverId = -1l;
	// this is http servers
	public static Map<Long, String> servers = new ConcurrentHashMap<Long, String>();
		
	public String destRoot = "./mm_data/";

	private Set<String> storeArray = new HashSet<String>();
	
	private String sysInfoServerName = null;
	private int sysInfoServerPort = -1;
	
	public enum RedisMode {
		SENTINEL, STANDALONE,
	}
	
	private RedisMode redisMode;
	private Set<String> sentinels;
	private String outsideIP;
	private boolean isHTTPOnly = false;
	
	private boolean indexFeatures = false;
	
	private int redisTimeout = 30 * 1000;
	
	private boolean isSSMaster = false;
	
	private boolean enableSSMig = false;
	
	private static long ss_id = -1L;
	
	private static long ckpt_ts = -1L;
	
	private String lmdb_prefix = ".";
	
	// FIXME: BUG-XXX: Auto migrate kvs and clean mm.dedup.info when memory exceeds 
	// redis' used_memory * memfull_ratio
	private double memFullRatio = 0.6;
	
	private long memorySize = 32 * 1024; // in MB
	
	private long memCheckInterval = 60 * 1000; // 60 seconds
	
	private int di_keep_days = 3; // default only keep dupinfo for 3 days

	public ServerConf(int httpPort) throws Exception {
		this.nodeName = InetAddress.getLocalHost().getHostName();
		this.httpPort = httpPort;
		setRedisMode(RedisMode.SENTINEL);
	}
	
	public ServerConf(String nodeName, int serverPort, Set<String> sentinels, 
			int blockSize, int period, int httpPort) throws Exception {
		if (nodeName == null)
			this.nodeName = InetAddress.getLocalHost().getHostName();
		else
			this.nodeName = nodeName;
		if (serverPort > 0)
			this.serverPort = serverPort;

		this.blockSize = blockSize;
		this.period = period;
		this.httpPort = httpPort;
		this.sentinels = sentinels;
		setRedisMode(RedisMode.SENTINEL);
		
		// ok, get global config if they exist.
		Jedis jedis = new RedisFactory(this).getDefaultInstance();
		if (jedis == null)
			throw new JedisException("Get default jedis instance failed.");
		
		Pipeline p = jedis.pipelined();
		p.get("mm.conf.blocksize");
		p.get("mm.conf.period");
		List<Object> results = p.syncAndReturnAll();
		if (results.get(0) != null) {
			this.blockSize = Integer.parseInt(results.get(1).toString());
			System.out.println("Get blockSize from redis server: " + this.blockSize);
		}
		if (results.get(1) != null) {
			this.period = Integer.parseInt(results.get(2).toString());
			System.out.println("Get period from redis server: " + this.period);
		}
		jedis.disconnect();
	}
	
	public ServerConf(String nodeName, int serverPort, String redisHost, int redisPort, 
			int blockSize, int period, int httpPort) throws Exception {
		if (nodeName == null)
			this.nodeName = InetAddress.getLocalHost().getHostName();
		else
			this.nodeName = nodeName;
		if (serverPort > 0)
			this.serverPort = serverPort;
		if (redisHost == null)
            this.redisHost = this.nodeName;
		else
            this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.blockSize = blockSize;
		this.period = period;
		this.httpPort = httpPort;
		setRedisMode(RedisMode.STANDALONE);
		
		// ok, get global config if they exist.
		Jedis jedis = new RedisFactory(this).getDefaultInstance();
		if (jedis == null)
			throw new JedisException("Get default jedis instance failed.");
		
		Pipeline p = jedis.pipelined();
		p.get("mm.conf.blocksize");
		p.get("mm.conf.period");
		List<Object> results = p.syncAndReturnAll();
		if (results.get(0) != null) {
			this.blockSize = Integer.parseInt(results.get(1).toString());
			System.out.println("Get blockSize from redis server: " + this.blockSize);
		}
		if (results.get(1) != null) {
			this.period = Integer.parseInt(results.get(2).toString());
			System.out.println("Get period from redis server: " + this.period);
		}
		jedis = RedisFactory.putInstance(jedis);
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String name) {
		this.nodeName = name;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public String getRedisHost() {
        return redisHost;
	}
	
	public void setRedisHost(String redisHost) {
	        this.redisHost = redisHost;
	}
	
	public int getRedisPort() {
	        return redisPort;
	}
	
	public void setRedisPort(int redisPort) {
	        this.redisPort = redisPort;
	}
	

	public int getHttpPort(){
		return httpPort;
	}
	
	public void setHttpPort(int httpPort){
		this.httpPort = httpPort;
	}
	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public int getPeriod() {
		return period;
	}

	public void setPeriod(int period) {
		this.period = period;
	}


	public int getFlush_interval() {
		return flush_interval;
	}


	public void setFlush_interval(int flush_interval) {
		this.flush_interval = flush_interval;
	}


	public int getReqnr_to_flush() {
		return reqnr_to_flush;
	}


	public void setReqnr_to_flush(int reqnr_to_flush) {
		this.reqnr_to_flush = reqnr_to_flush;
	}


	public boolean isUse_junixsocket() {
		return use_junixsocket;
	}


	public void setUse_junixsocket(boolean use_junixsocket) {
		this.use_junixsocket = use_junixsocket;
	}


	public Set<String> getStoreArray() {
		return storeArray;
	}


	public void setStoreArray(Set<String> storeArray) {
		this.storeArray = storeArray;
	}


	public RedisMode getRedisMode() {
		return redisMode;
	}


	public void setRedisMode(RedisMode redisMode) {
		this.redisMode = redisMode;
	}


	public Set<String> getSentinels() {
		return sentinels;
	}


	public void setSentinels(Set<String> sentinels) {
		this.sentinels = sentinels;
	}

	public String getSysInfoServerName() {
		return sysInfoServerName;
	}

	public void setSysInfoServerName(String sysInfoServerName) {
		this.sysInfoServerName = sysInfoServerName;
	}

	public int getSysInfoServerPort() {
		return sysInfoServerPort;
	}

	public void setSysInfoServerPort(int sysInfoServerPort) {
		this.sysInfoServerPort = sysInfoServerPort;
	}

	public String getOutsideIP() {
		return outsideIP;
	}

	public void setOutsideIP(String outsideIP) {
		this.outsideIP = outsideIP;
	}

	public boolean isHTTPOnly() {
		return isHTTPOnly;
	}

	public void setHTTPOnly(boolean isHTTPOnly) {
		this.isHTTPOnly = isHTTPOnly;
	}

	public long getWrite_fd_recycle_to() {
		return write_fd_recycle_to;
	}

	public void setWrite_fd_recycle_to(long write_fd_recycle_to) {
		this.write_fd_recycle_to = write_fd_recycle_to;
	}

	public long getRead_fd_recycle_to() {
		return read_fd_recycle_to;
	}

	public void setRead_fd_recycle_to(long read_fd_recycle_to) {
		this.read_fd_recycle_to = read_fd_recycle_to;
	}

	public String getFeatureIndexPath() {
		return featureIndexPath;
	}

	public void setFeatureIndexPath(String featureIndexPath) {
		this.featureIndexPath = featureIndexPath + "/feature_index";
	}

	public List<FeatureType> getFeatures() {
		return features;
	}

	public void addToFeatures(String features) {
		if (features.equalsIgnoreCase(FeatureTypeString.IMAGE_PHASH_ES))
			ServerConf.features.add(FeatureType.IMAGE_PHASH_ES);
		if (features.equalsIgnoreCase(FeatureTypeString.IMAGE_LIRE))
			ServerConf.features.add(FeatureType.IMAGE_LIRE);
		if (features.equalsIgnoreCase(FeatureTypeString.IMAGE_FACES))
			ServerConf.features.add(FeatureType.IMAGE_FACES);
	}
	
	public static String getFeatureTypeString(FeatureType type) {
		switch (type) {
		case IMAGE_PHASH_ES:
			return FeatureTypeString.IMAGE_PHASH_ES;
		case IMAGE_LIRE:
			return FeatureTypeString.IMAGE_LIRE;
		case IMAGE_FACES:
			return FeatureTypeString.IMAGE_FACES;
		default:
			break;
		}			
		return "none";
	}

	public boolean isIndexFeatures() {
		return indexFeatures;
	}

	public void setIndexFeatures(boolean indexFeatures) {
		this.indexFeatures = indexFeatures;
	}

	public String getFaceDetectorXML() {
		return faceDetectorXML;
	}

	public void setFaceDetectorXML(String faceDetectorXML) {
		this.faceDetectorXML = faceDetectorXML;
	}

	public int getRedisTimeout() {
		return redisTimeout;
	}

	public void setRedisTimeout(int redisTimeout) {
		this.redisTimeout = redisTimeout;
	}

	public boolean isSSMaster() {
		return isSSMaster;
	}

	public void setSSMaster(boolean isSSMaster) {
		this.isSSMaster = isSSMaster;
	}

	public static long getSs_id() {
		return ss_id;
	}

	public static void setSs_id(long ss_id) {
		ServerConf.ss_id = ss_id;
	}

	public static long getCkpt_ts() {
		return ckpt_ts;
	}

	public static void setCkpt_ts(long ckpt_ts) {
		ServerConf.ckpt_ts = ckpt_ts;
	}
	
	public String getLmdb_prefix() {
		return lmdb_prefix;
	}
	
	public void setLmdb_prefix(String lmdb_prefix) {
		this.lmdb_prefix = lmdb_prefix;
	}

	public double getMemFullRatio() {
		return memFullRatio;
	}

	public void setMemFullRatio(double memFullRatio) {
		this.memFullRatio = memFullRatio;
	}

	public long getMemorySize() {
		return memorySize * 1024 * 1024;
	}

	/**
	 * @param memorySize is in MB
	 */
	public void setMemorySize(long memorySize) {
		this.memorySize = memorySize;
	}

	public long getMemCheckInterval() {
		return memCheckInterval;
	}

	public void setMemCheckInterval(long memCheckInterval) {
		this.memCheckInterval = memCheckInterval;
	}

	public int getDi_keep_days() {
		return di_keep_days;
	}

	public void setDi_keep_days(int di_keep_days) {
		this.di_keep_days = di_keep_days;
	}

	public boolean isEnableSSMig() {
		return enableSSMig;
	}

	public void setEnableSSMig(boolean enableSSMig) {
		this.enableSSMig = enableSSMig;
	}

	public boolean isVerbose(int level) {
		return verbose >= level;
	}

	public void setVerbose(int verbose) {
		this.verbose = verbose;
	}

	public static int getRecv_buffer_size() {
		return recv_buffer_size;
	}

	public static void setRecv_buffer_size(int recv_buffer_size) {
		ServerConf.recv_buffer_size = recv_buffer_size;
	}

	public static int getSend_buffer_size() {
		return send_buffer_size;
	}

	public static void setSend_buffer_size(int send_buffer_size) {
		ServerConf.send_buffer_size = send_buffer_size;
	}
}
