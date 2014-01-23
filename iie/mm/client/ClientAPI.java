package iie.mm.client;

import iie.mm.client.ClientConf.RedisInstance;
import iie.mm.client.PhotoClient.SocketHashEntry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

public class ClientAPI {
	private PhotoClient pc;
	private int index;				
	private List<String> keyList = new ArrayList<String>();
	//缓存与服务端的tcp连接,服务端名称到连接的映射
	private Map<String, SocketHashEntry> socketHash;
	private Jedis jedis;
	
	public ClientAPI(ClientConf conf) {
		pc = new PhotoClient(conf);
	}

	public ClientAPI() {
		pc = new PhotoClient();
	}
	
	/**
	 * DO NOT USE this function unless if you know what are you doing.
	 * @return PhotoClient
	 */
	public PhotoClient getPc() {
		return pc;
	}
	
	private void updateClientConf(Jedis jedis, ClientConf conf) {
		if (conf == null || !conf.isAutoConf() || jedis == null)
			return;
		try {
			String dupMode = jedis.hget("mm.client.conf", "dupmode");
			if (dupMode != null) {
				if (dupMode.equalsIgnoreCase("dedup")) {
					conf.setMode(ClientConf.MODE.DEDUP);
				} else if (dupMode.equalsIgnoreCase("nodedup")) {
					conf.setMode(ClientConf.MODE.NODEDUP);
				}
			}
			String dupNum = jedis.hget("mm.client.conf", "dupnum");
			if (dupNum != null) {
				int dn = Integer.parseInt(dupNum);
				if (dn > 1)
					conf.setDupNum(dn);
			}
			String sockPerServer = jedis.hget("mm.client.conf", "sockperserver");
			if (sockPerServer != null) {
				int sps = Integer.parseInt(sockPerServer);
				if (sps >= 1)
					conf.setSockPerServer(sps);
			}
			System.out.println("Auto conf client with: dupMode=" + dupMode + 
					", dupNum=" + conf.getDupNum() + ", sockPerServer=" + 
					conf.getSockPerServer());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private int init_by_sentinel(ClientConf conf, String urls) throws Exception {
		if (conf.getRedisMode() != ClientConf.RedisMode.SENTINEL) {
			return -1;
		}
		// iterate the sentinel set, get master IP:port, save to RedisFactory
		if (conf.getSentinels() == null) {
			if (urls == null) {
				throw new Exception("Invalid URL or sentinels.");
			}
			HashSet<String> sens = new HashSet<String>();
			String[] s = urls.split(";");
			
			for (int i = 0; i < s.length; i++) {
				sens.add(s[i]);
			}
			conf.setSentinels(sens);
		}
		jedis = pc.getRf().getNewInstance(null);
		updateClientConf(jedis, conf);
		
		return 0;
	}
	
	private int init_by_standalone(ClientConf conf, String urls) throws Exception {
		String[] x = urls.split(";");
		for (String url : x) {
			String[] redishp = url.split(":"); 
			if (redishp.length != 2)
				throw new Exception("wrong format of url: " + url);
			
			if (pc.getJedis() == null) {
				pc.getRf();
				jedis = RedisFactory.getRawInstance(redishp[0], Integer.parseInt(redishp[1]));
				pc.setJedis(jedis);
			} else {
				// Use the Redis Server in conf
				jedis = pc.getJedis();
			}
			conf.setRedisInstance(new RedisInstance(redishp[0], Integer.parseInt(redishp[1])));
		}
		updateClientConf(jedis, conf);
		
		return 0;
	}

	/**
	 * 连接服务器,进行必要初始化,并与redis服务器建立连接
	 * 如果初始化本对象时传入了conf，则使用conf中的redis地址，否则使用参数url
	 * It is not thread-safe!
	 * @param url redis的主机名:端口
	 * @return 
	 */
	public int init(String urls) throws Exception {
		//与jedis建立连接
		if (urls == null) {
			throw new Exception("The url can not be null.");
		}
		
		if (pc.getConf() == null) {
			pc.setConf(new ClientConf());
		}
		pc.getConf().clrRedisIns();
		if (urls.startsWith("STL://")) {
			urls = urls.substring(6);
			pc.getConf().setRedisMode(ClientConf.RedisMode.SENTINEL);
		} else if (urls.startsWith("STA://")) {
			urls = urls.substring(6);
			pc.getConf().setRedisMode(ClientConf.RedisMode.STANDALONE);
		}
		switch (pc.getConf().getRedisMode()) {
		case SENTINEL:
			init_by_sentinel(pc.getConf(), urls);
			break;
		case STANDALONE:
			init_by_standalone(pc.getConf(), urls);
			break;
		}
		
		socketHash = new ConcurrentHashMap<String, SocketHashEntry>();
		//从redis上获取所有的服务器地址
		Set<Tuple> active = jedis.zrangeWithScores("mm.active", 0, -1);
		if (active != null && active.size() > 0) {
			for (Tuple t : active) {
				pc.addToServers((long)t.getScore(), t.getElement());
			}
			for (Tuple t : active) {
				String[] c = t.getElement().split(":");
				if (c.length == 2) {
					Socket sock = new Socket();
					SocketHashEntry she = new SocketHashEntry(c[0], Integer.parseInt(c[1]), pc.getConf().getSockPerServer());
					try {
						sock.setTcpNoDelay(true);
						sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));
						she.addToSockets(sock, new DataInputStream(sock.getInputStream()),
								new DataOutputStream(sock.getOutputStream()));
						socketHash.put(t.getElement(), she);
					} catch (SocketException e) {
						e.printStackTrace();
						continue;
					} catch (NumberFormatException e) {
						e.printStackTrace();
						continue;
					} catch (IOException e) {
						e.printStackTrace();
						continue;
					}
				}
			}
		}
		System.out.println("Got active server size=" + socketHash.size());
		keyList.addAll(socketHash.keySet());
		pc.setSocketHash(socketHash);
		return 0;
	}
	
	/**
	 * 同步写,对外提供的接口
	 * It is thread-safe!
	 * @param set
	 * @param md5
	 * @param content
	 * @return		
	 */
	public String put(String key, byte[] content) throws IOException, Exception {
		if (key == null || keyList.size() ==0)
			throw new Exception("key can not be null or MetaError.");
		String[] keys = key.split("@");
		if (keys.length != 2)
			throw new Exception("wrong format of key:" + key);
		String r = null;
		boolean nodedup = false;
		for (int i = 0; i < pc.getConf().getDupNum(); i++) {
			SocketHashEntry she = socketHash.get(keyList.get((index + i) % keyList.size()));
			if (she.probSelected())
				// BUG-XXX: we have to check if we can recover from this exception, 
				// then try our best to survive.
				try {
					r = pc.syncStorePhoto(keys[0], keys[1], content, she, nodedup);
					if (r.split("#").length < pc.getConf().getDupNum()) {
						nodedup = true;
					} else 
						nodedup = false;
				} catch (SocketException e) {
					i--;
					index++;
				}
			else {
				i--;
				index++;
			}
		}
		index++;
		if (index >= keyList.size()) {
			index = 0;
		}
		return r;
	}
	
	/**
	 * 异步写,对外提供的接口
	 * @param set
	 * @param md5
	 * @param content
	 * @return		
	 */
	public void iPut(String key, byte[] content)  throws IOException, Exception{
		/*if(key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("@");
		if(keys.length != 2)
			throw new Exception("wrong format of key:"+key);
		for (int i = 0; i < pc.getConf().getDupNum(); i++) {
		//	Socket sock = socketHash.get(keyList.get((index + i) % keyList.size()));
		//	pc.asyncStorePhoto(keys[0], keys[1], content, sock);
		}
		index++;
		if(index >= socketHash.size()){
			index = 0;
		}*/
	}
	
	/**
	 * 批量同步写,对外提供的接口
	 * @param set
	 * @param md5
	 * @param content
	 * @return		
	 */
	public String[] mPut(String set,String[] md5s, byte[][] content) throws Exception
	{	
		if (set == null || md5s.length == 0 || content.length == 0){
			throw new Exception("set or md5s or contents can not be null.");
		}else if(md5s.length != content.length)
			throw new  Exception("arguments length mismatch.");
		String[] r = null;
		for (int i = 0; i < pc.getConf().getDupNum(); i++) {
//			Socket sock = socketHash.get(keyList.get((index + i) % keyList.size()));
//			r = pc.mPut(set, md5s, content, sock);
			SocketHashEntry she = socketHash.get(keyList.get((index + i) % keyList.size()));
			r = pc.mPut(set, md5s, content, she);
		}
		index++;
		if (index >= keyList.size()){
			index = 0;
		}
		return r;
	}
	
	/**
	 * 批量异步写，对外提供的接口
	 * @param key	redis中的键以set开头+#+md5的字符串形成key
	 * @return		图片内容,如果图片不存在则返回长度为0的byte数组
	 */
	
	public void imPut(String[] keys, byte[][] contents) throws Exception {
		/*if(keys.length != contents.length){
			throw new Exception("keys's length is not the same as contents'slength.");
		}
		for(int i = 0;i<keys.length;i++){
			iPut(keys[i],contents[i]);
		}*/
	}
	
	/**
	 * It is thread-safe
	 * 同步取，对外提供的接口
	 * @param key	或者是set@md5,或者是文件元信息，可以是拼接后的
	 * @return		图片内容,如果图片不存在则返回长度为0的byte数组
	 */
	public byte[] get(String key) throws IOException, Exception {
		if (key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("@|#");
		if (keys.length == 2)
			return pc.getPhoto(keys[0], keys[1]);
		else if (keys.length == 7)
			return pc.searchByInfo(key, keys);
		else if (keys.length % 7 == 0)		//如果是拼接的元信息，分割后长度是7的倍数
			return pc.searchPhoto(key);
		else 
			throw new Exception("wrong format of key:" + key);
	}

	public void quit() {
		if (pc.getRf() != null) {
			pc.getRf().putInstance(jedis);
			pc.getRf().quit();
		}
		pc.close();
	}
}
