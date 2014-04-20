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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

public class ClientAPI {
	private PhotoClient pc;
	private AtomicInteger index = new AtomicInteger(0);				
	private List<String> keyList = Collections.synchronizedList(new ArrayList<String>());
	//缓存与服务端的tcp连接,服务端名称到连接的映射
	private ConcurrentHashMap<String, SocketHashEntry> socketHash;
	private Jedis jedis;
	private final Timer timer = new Timer("ActiveMMSRefresher");
	
	public ClientAPI(ClientConf conf) {
		pc = new PhotoClient(conf);
	}

	public ClientAPI() {
		pc = new PhotoClient();
	}
	
	public enum MMType {
		TEXT, IMAGE, AUDIO, VIDEO, APPLICATION, THUMBNAIL, OTHER,
	}
	
	public static String getMMTypeSymbol(MMType type) {
		switch (type) {
		case TEXT:
			return "t";
		case IMAGE:
			return "i";
		case AUDIO:
			return "a";
		case VIDEO:
			return "v";
		case APPLICATION:
			return "o";
		case THUMBNAIL:
			return "s";
		case OTHER:
		default:
			return "";
		}
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
			String dupinfo = jedis.hget("mm.client.conf", "dupinfo");
			if (dupinfo != null) {
				int di = Integer.parseInt(dupinfo);
				if (di > 0)
					conf.setLogDupInfo(true);
			}
			String mgetTimeout = jedis.hget("mm.client.conf", "mgetto");
			if (mgetTimeout != null) {
				int to = Integer.parseInt(mgetTimeout);
				if (to > 0)
					conf.setMgetTimeout(to * 1000);
			}
			System.out.println("Auto conf client with: dupMode=" + dupMode + 
					", dupNum=" + conf.getDupNum() + 
					", logDupInfo=" + conf.isLogDupInfo() +  
					", sockPerServer=" + conf.getSockPerServer() +
					", mgetTimeout=" + conf.getMgetTimeout());
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
	
	private boolean refreshActiveMMS(boolean isInit) {
		if (isInit) {
			return getActiveMMS();
		} else {
			try {
				List<String> active = pc.getActiveMMSByHB();
				Set<String> activeMMS = new TreeSet<String>();
				
				// BUG-XXX: getActiveMMSByHB() return the DNSed server name, it might be different
				// with saved server name. Thus, if a server changes its ip address, we can't
				// find it even we put it into socketHash.
				// Possibly, we get server_name:server_ip pair, and try to find by name and ip.
				// If server_name in servers, check up socketHash.get(server_name) {free it?}
				// and check up socketHash.get(server_ip), finally update server_ip to servers?
				// else if server_ip in servers, it is ok.
				// else register ourself to servers?
				if (active.size() > 0) {
					for (String a : active) {
						String[] c = a.split(":");

						if (c.length == 2) {
							if (socketHash.get(a) == null) {
								// new MMS?
								Socket sock = new Socket();
								SocketHashEntry she = new SocketHashEntry(c[0], Integer.parseInt(c[1]), pc.getConf().getSockPerServer());
								try {
									sock.setTcpNoDelay(true);
									sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));
									she.addToSockets(sock, new DataInputStream(sock.getInputStream()),
											new DataOutputStream(sock.getOutputStream()));
									if (socketHash.putIfAbsent(a, she) != null) {
										she.clear();
									}
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
							activeMMS.add(a);
						}
					}
					synchronized (keyList) {
						keyList.clear();
						keyList.addAll(activeMMS);
						keyList.retainAll(socketHash.keySet());
					}
				} else {
					keyList.clear();
				}
				System.out.println("Refresh active servers: " + keyList);
			} catch (IOException e) {
			}
		}
		return true;
	}
	
	private boolean getActiveMMS() {
		Set<Tuple> active = jedis.zrangeWithScores("mm.active", 0, -1);
		Set<String> activeMMS = new TreeSet<String>();
		
		if (active != null && active.size() > 0) {
			for (Tuple t : active) {
				// translate ServerName to IP address
				String ipport = jedis.hget("mm.dns", t.getElement());

				// update server ID->Name map
				if (ipport == null) {
					pc.addToServers((long)t.getScore(), t.getElement());
					ipport = t.getElement();
				} else
					pc.addToServers((long)t.getScore(), ipport);
				
				String[] c = ipport.split(":");
				if (c.length == 2 && socketHash.get(ipport) == null) {
					Socket sock = new Socket();
					SocketHashEntry she = new SocketHashEntry(c[0], Integer.parseInt(c[1]), pc.getConf().getSockPerServer());
					activeMMS.add(ipport);
					try {
						sock.setTcpNoDelay(true);
						sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));
						she.addToSockets(sock, new DataInputStream(sock.getInputStream()),
								new DataOutputStream(sock.getOutputStream()));
						if (socketHash.putIfAbsent(ipport, she) != null) {
							she.clear();
						}
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
		synchronized (keyList) {
			keyList.addAll(activeMMS);
			keyList.retainAll(socketHash.keySet());
		}
		
		return true;
	}
	
	private class MMCTimerTask extends TimerTask {
		@Override
		public void run() {
			try {
				refreshActiveMMS(false);
			} catch (Exception e) {
			}
		}
	}

	/**
	 * 连接服务器,进行必要初始化,并与redis服务器建立连接
	 * 如果初始化本对象时传入了conf，则使用conf中的redis地址，否则使用参数url
	 * It is not thread-safe!
	 * 
	 * @param url MM STANDARD PROTOCOL: STL:// or STA://
	 * @return
	 * @throws Exception
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
		refreshActiveMMS(true);
		System.out.println("Got active server size=" + keyList.size());
		pc.setSocketHash(socketHash);
		
		timer.schedule(new MMCTimerTask(), 500, 5000);
		
		return 0;
	}
	
	private String __put(Set<String> targets, String[] keys, byte[] content, boolean nodedup) 
			throws Exception {
		Random rand = new Random();
		String r = null;
		Set<String> saved = new TreeSet<String>();
		HashMap<String, Long> failed = new HashMap<String, Long>();
		int targetnr = targets.size(); 
		
		do {
			for (String server : targets) {
				SocketHashEntry she = socketHash.get(server);
				if (she.probSelected()) {
					// BUG-XXX: we have to check if we can recover from this exception, 
					// then try our best to survive.
					try {
						r = pc.syncStorePhoto(keys[0], keys[1], content, she, nodedup);
						if (r.split("#").length < pc.getConf().getDupNum()) {
							nodedup = true;
						} else 
							nodedup = false;
						saved.add(server);
					} catch (Exception e) {
						if (failed.containsKey(server)) {
							failed.put(server, failed.get(server) + 1);
						} else
							failed.put(server, new Long(1));
						System.out.println("[PUT] " + keys[0] + "@" + keys[1] + " to " + server + 
								" failed: (" + e.getMessage() + ") for " + failed.get(server) + " times.");
					}
				} else {
					// this means target server has no current usable connection, we try to use 
					// another server
				}
			}
			if (saved.size() < targetnr) {
				List<String> remains = new ArrayList<String>(keyList);
				remains.removeAll(saved);
				for (Map.Entry<String, Long> e : failed.entrySet()) {
					if (e.getValue() > 9) {
						remains.remove(e.getKey());
					}
				}
				if (remains.size() == 0)
					break;
				targets.clear();
				for (int i = saved.size(); i < targetnr; i++) {
					targets.add(remains.get(rand.nextInt(remains.size())));
				}
			} else break;
		} while (true);
		
		if (saved.size() == 0) {
			throw new Exception("Error in saving Key: " + keys[0] + "@" + keys[1]);
		}
		return r;
	}
	
	/**
	 * 同步写,对外提供的接口
	 * It is thread-safe!
	 * 
	 * @param key
	 * @param content
	 * @return info to locate the file	
	 * @throws Exception
	 */
	public String put(String key, byte[] content) throws Exception {
		if (key == null || keyList.size() == 0) {
			throw new Exception("Key can not be null or no active MMServer (" + keyList.size() + ").");
		}
		String[] keys = key.split("@");
		if (keys.length != 2)
			throw new Exception("Wrong format of key: " + key);
		boolean nodedup = false;
		int dupnum = Math.min(keyList.size(), pc.getConf().getDupNum());
		
		// roundrobin select dupnum servers from keyList, if error in put, random select in remain servers
		Set<String> targets = new TreeSet<String>();
		int idx = index.getAndIncrement();
		if (idx < 0) {
			index.compareAndSet(idx, 0);
			idx = index.get();
		}
		for (int i = 0; i < dupnum; i++) {
			targets.add(keyList.get((idx + i) % keyList.size()));
		}
		
		return __put(targets, keys, content, nodedup);
	}
	
	/**
	 * 异步写,对外提供的接口（暂缓使用）
	 * 
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
	 * 批量同步写,对外提供的接口（暂缓使用）
	 * 
	 * @param set
	 * @param md5
	 * @param content
	 * @return		
	 */
	public String[] mPut(String set,String[] md5s, byte[][] content) throws Exception {	
		if (set == null || md5s.length == 0 || content.length == 0){
			throw new Exception("set or md5s or contents can not be null.");
		}else if(md5s.length != content.length)
			throw new  Exception("arguments length mismatch.");
		String[] r = null;
		int idx = index.getAndIncrement();
		
		if (idx < 0) {
			index.compareAndSet(idx, 0);
			idx = index.get();
		}
		for (int i = 0; i < pc.getConf().getDupNum(); i++) {
			SocketHashEntry she = socketHash.get(keyList.get((idx + i) % keyList.size()));
			r = pc.mPut(set, md5s, content, she);
		}

		return r;
	}
	
	/**
	 * 批量异步写，对外提供的接口（暂缓使用）
	 * 
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
	 * 根据info从系统中读取对象内容，重新写入到MMServer上仅当info中活动的MMServer个数小于dupnum时
	 * （should only be used by system tools）
	 * 
	 * @param key
	 * @param info
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
    public String xput(String key, String info) throws IOException, Exception {                                                                                                                                                                       
        if (key == null || keyList.size() == 0) {
            throw new Exception("Key can not be null or no active MMServer (" + keyList.size() + ").");
        }
        String[] keys = key.split("@");
        if (keys.length != 2)
            throw new Exception("Wrong format of key: " + key);

        byte[] content = this.get(info);
        boolean nodedup = false;
        int dupnum = Math.min(keyList.size(), pc.getConf().getDupNum());

        if (content == null) {
        	throw new IOException("Try to get content of KEY: " + key + " failed.");
        }
        // roundrobin select dupnum servers from keyList, if error in put, random select in remain servers
        TreeSet<String> targets = new TreeSet<String>();
        Set<String> active = new TreeSet<String>();
        targets.addAll(keyList);

        String[] infos = info.split("#");
        for (String s : infos) {
            long sid = Long.parseLong(s.split("@")[2]);
            String server = this.getPc().getServers().get(sid);
            if (server != null) {
                targets.remove(server);
                active.add(server);
            }
        }
        if (active.size() >= dupnum)
        	return info;
        if (targets.size() == 0)
            throw new IOException("No more copy can be made: " + key + " --> " + info);
        else if (targets.size() > (dupnum - active.size())) {
        	Random rand = new Random();
        	String[] tArray = targets.toArray(new String[0]);
        	targets.clear();
        	for (int i = 0; i < dupnum - active.size(); i++) {
        		targets.add(tArray[rand.nextInt(tArray.length)]);
        	}
        }

        return __put(targets, keys, content, nodedup);
    }
	
	/**
	 * 同步取，对外提供的接口
	 * It is thread-safe
	 * 
	 * @param key	或者是set@md5,或者是文件元信息，可以是拼接后的
	 * @return		图片内容,如果图片不存在则返回长度为0的byte数组
     * @throws IOException
     * @throws Exception
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
	
	/**
	 * 批量读取某个集合的所有key
	 * It is thread-safe
	 *
	 * @param type
	 * @param begin_time
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	public List<String> getkeys(String type, long begin_time) throws IOException, Exception {
		String prefix;
		
		if (type == null)
			throw new Exception("type can not be null");
		if (type.equalsIgnoreCase("image")) {
			prefix = getMMTypeSymbol(MMType.IMAGE);
		} else if (type.equalsIgnoreCase("thumbnail")) {
			prefix = getMMTypeSymbol(MMType.THUMBNAIL);
		} else if (type.equalsIgnoreCase("text")) {
			prefix = getMMTypeSymbol(MMType.TEXT);
		} else if (type.equalsIgnoreCase("audio")) {
			prefix = getMMTypeSymbol(MMType.AUDIO);
		} else if (type.equalsIgnoreCase("video")) {
			prefix = getMMTypeSymbol(MMType.VIDEO);
		} else if (type.equalsIgnoreCase("application")) {
			prefix = getMMTypeSymbol(MMType.APPLICATION);
		} else if (type.equalsIgnoreCase("other")) {
			prefix = getMMTypeSymbol(MMType.OTHER);
		} else {
			throw new Exception("type '" + type + "' is invalid.");
		}
		
		// query on redis
		TreeSet<Long> tranges = pc.getSets(prefix);
		try {
			long setTs = tranges.tailSet(begin_time).first();
			return pc.getSetElements(prefix + setTs);
		} catch (NoSuchElementException e) {
			throw new Exception("Can not find any keys larger or equal to " + begin_time);
		}
	}
	
	/**
	 * 批量获取keys对应的所有多媒体对象内容
	 * it is thread safe.
	 * 
	 * @param keys
	 * @param cookies
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	public List<byte[]> mget(List<String> keys, Map<String, String> cookies) throws IOException, Exception {
		if (keys == null || cookies == null)
			throw new Exception("keys or cookies list can not be null.");
		if (keys.size() > 0) {
			String key = keys.get(keys.size() - 1);
			if (key != null) {
				long ts = -1;
				try {
					ts = Long.parseLong(key.split("@")[0].substring(1));
				} catch (Exception e) {
				}
				cookies.put("ts", Long.toString(ts));
			}
		}
		return pc.mget(keys, cookies);
	}
	
	/**
	 * 退出多媒体客户端，释放内部资源
	 */
	public void quit() {
		if (pc.getRf() != null) {
			pc.getRf().putInstance(jedis);
			pc.getRf().quit();
		}
		pc.close();
		timer.cancel();
	}
}
