package iie.mm.client;

import iie.mm.client.PhotoClient.SocketHashEntry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import redis.clients.jedis.Jedis;

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
	
	public PhotoClient getPc() {
		return pc;
	}

	/**
	 * 连接服务器,进行必要初始化,并与redis服务器建立连接
	 * 如果初始化本对象时传入了conf，则使用conf中的redis地址，否则使用参数url
	 * It is not thread-safe!
	 * @param url redis的主机名:端口
	 * @return 
	 */
	public int init(String url) throws Exception {
		//与jedis建立连接
		if (url == null) {
			throw new Exception("The url can not be null.");
		}
		String[] redishp = url.split(":"); 
		if (redishp.length != 2)
			throw new Exception("wrong format of url: " + url);
		
		if (pc.getConf() == null) {
			pc.setConf(new ClientConf());
		}
		if (pc.getJedis() == null) {
			jedis = RedisFactory.getNewInstance(redishp[0], Integer.parseInt(redishp[1]));
			pc.setJedis(jedis);
		} else {
			// Use the Redis Server in conf
			jedis = pc.getJedis();
		}
		pc.getConf().setRedisHost(redishp[0]);
		pc.getConf().setRedisPort(Integer.parseInt(redishp[1]));
		
		socketHash = new ConcurrentHashMap<String, SocketHashEntry>();
		//从redis上获取所有的服务器地址
		Set<String> active = jedis.zrange("mm.active", 0, -1);
		if (active != null && active.size() > 0) {
			String[] aa = active.toArray(new String[0]);
			for (int i = 0; i < aa.length; i++) {
				pc.addToServers(i, aa[i]);
			}
			for (String s : active) {
				String[] c = s.split(":");
				if (c.length == 2) {
					@SuppressWarnings("resource")
					Socket sock = new Socket();
					SocketHashEntry she = new SocketHashEntry();
					try {
						sock.setTcpNoDelay(true);//不要延迟
						sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));//本地与所有的服务器相连
						she.dis = new DataInputStream(sock.getInputStream());
						she.dos = new DataOutputStream(sock.getOutputStream());
						she.socket = sock;
						socketHash.put(s, she);
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
		keyList.addAll(socketHash.keySet());
		pc.setSocketHash(socketHash);
		return 0;
	}
	
	/**
	 * 同步写,对外提供的接口
	 * @param set
	 * @param md5
	 * @param content
	 * @return		
	 */
	public String put(String key, byte[] content) throws IOException, Exception {
		if (key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("@");
		if (keys.length != 2)
			throw new Exception("wrong format of key:" + key);
		String r = null;
		for (int i = 0; i < pc.getConf().getDupNum(); i++) {
			SocketHashEntry she = socketHash.get(keyList.get((index + i) % keyList.size()));
			r = pc.syncStorePhoto(keys[0], keys[1], content, she);
		}
		index++;
		if (index >= keyList.size()) {
			index = 0;
		}
		return r;
	}
	
	/**
	 * 
	 * @param key	或者是set@md5,或者是文件元信息，可以是拼接后的
	 * @return		图片内容,如果图片不存在则返回长度为0的byte数组
	 */
	public byte[] get(String key) throws IOException, Exception {
		if (key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("@");
		if (keys.length == 2)
			return pc.getPhoto(keys[0], keys[1]);
		else if (keys.length == 7)
			return pc.searchByInfo(key, keys);
		else if (keys.length % 7 == 0)		//如果是拼接的元信息，分割后长度是7的倍数
			return pc.searchPhoto(key);
		else 
			throw new Exception("wrong format of key:" + key);
	}

}
