package iie.mm.client;

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
	private ClientConf conf;
	private int index;					
	private List<String> keyList = new ArrayList<String>();
	
	//缓存与服务端的tcp连接,服务端名称到连接的映射
	private Map<String, Socket> socketHash;
	private Jedis jedis;
	
	public ClientAPI(ClientConf conf) {
		this.conf = conf;
		pc = new PhotoClient(conf);
	}

	public ClientAPI() {
	}


	/**
	 * 连接服务器,进行必要初始化,并与redis服务器建立连接
	 * 如果初始化本对象时传入了conf，则使用conf中的redis地址，否则使用参数url
	 * It is not thread-safe!
	 * @param url redis的主机名#端口
	 * @return 
	 */
	public int init(String url) throws Exception{
		//与jedis建立连接
		if(conf == null)
		{
			if(url == null)
			{
				throw new Exception("The url can not be null.");
			}
			String[] redishp = url.split("#"); 
			if(redishp.length != 2)
				throw new Exception("wrong format of url:"+url);
			jedis = RedisFactory.getNewInstance(redishp[0], Integer.parseInt(redishp[1]));
			pc.setJedis(jedis);
		}
		else {
			jedis = RedisFactory.getNewInstance(conf.getRedisHost(),conf.getRedisPort());
		}
		
		socketHash = new ConcurrentHashMap<String, Socket>();
		Set<String> active = jedis.smembers("mm.active");//从redis上获取所有的服务器地址
		if (active != null && active.size() > 0) {
			for (String s : active) {
				String[] c = s.split("#");
				if (c.length == 2) {
					Socket sock = new Socket();
					try {
						sock.setTcpNoDelay(true);//不要延迟
						sock.connect(new InetSocketAddress(c[0], Integer.parseInt(c[1])));//本地与所有的服务器相连
						socketHash.put(s, sock);
					} catch (SocketException e) {
						e.printStackTrace();
						return -1;
					} catch (NumberFormatException e) {
						e.printStackTrace();
						return -1;
					} catch (IOException e) {
						e.printStackTrace();
						return -1;
					}
				}
				
			}
		}
		pc.setSocketHash(socketHash);
		return 1;
	}
	
	/**
	 * 同步写,对外提供的接口
	 * @param set
	 * @param md5
	 * @param content
	 * @return		
	 */
	public String put(String key, byte[] content)  throws IOException, Exception{
		if(key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("#");
		if(keys.length != 2)
			throw new Exception("wrong format of key:"+key);
		String setName = keys[0];
		String md5 = keys[1];
		keyList.addAll(socketHash.keySet());
		Socket sock = socketHash.get(keyList.get(index));
		String r = pc.syncStorePhoto(setName, md5, content,sock);
		index++;
		if(index >= socketHash.size()){
			index = 0;
		}
		return r;
	}
	
	public Map<String, String> getNrFromSet(String set) throws IOException {
		return jedis.hgetAll(set);
	}
	
	/**
	 * 
	 * @param key	redis中的键以set开头+#+md5的字符串形成key
	 * @return		图片内容,如果图片不存在则返回长度为0的byte数组
	 */
	public byte[] get(String key) throws Exception {
		if(key == null)
			throw new Exception("key can not be null.");
		String[] keys = key.split("#");
		if(keys.length == 2)
			return pc.getPhoto(keys[0],keys[1]);
		else if(keys.length == 8)
			return pc.searchPhoto(key);
		else 
			throw new Exception("wrong format of key:"+key);
	}
	

}
