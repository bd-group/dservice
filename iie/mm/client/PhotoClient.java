package iie.mm.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import redis.clients.jedis.Jedis;

public class PhotoClient {
	private Socket syncStoreSocket;			//用于同步写的socket
	private Socket asyncStoreSocket;	//用于异步写
	private ClientConf conf;
	private int serverPort = 0;

	
	private DataInputStream storeis;
	private DataOutputStream storeos;
	
	//缓存与服务端的tcp连接,服务端名称到连接的映射
	private Map<String, Socket> socketHash;
	//用于读请求的socket
	private Socket searchSocket;
	private Jedis jedis;
	
	/**
	 * 读取配置文件,进行必要初始化,并与redis服务器建立连接
	 * It is not thread-safe!
	 * @throws IOException 
	 */
	public PhotoClient(ClientConf conf) throws IOException {
		this.conf = conf;
		
		//连接服务器
		jedis = RedisFactory.getNewInstance(conf.getRedisHost(), conf.getRedisPort());
		socketHash = new ConcurrentHashMap<String, Socket>();

		if (conf.getServerPort() > 0)
			serverPort = conf.getServerPort();
		else {
			// get the local MM service (port) from redis server
			Set<String> active = jedis.smembers("mm.active");
			if (active != null && active.size() > 0) {
				for (String s : active) {
					if (s.startsWith(conf.getServerName())) {
						// ok, parse the port
						String[] c = s.split(":");
						if (c.length == 2) {
							serverPort = Integer.parseInt(c[1]);
							break;
						}
					}
				}
			}
		}
		if (serverPort == 0) {
			System.out.println("[WARN] Invalid mm server port for host " + conf.getServerName() + "'s storage.");
			throw new IOException("Invalid mm server port for host " + conf.getServerName() + "'s storage.");
		} else {
			System.out.println("[INFO] Resolve host " + conf.getServerName() + "'s port to " + serverPort);
		}
	}
	
	private String __syncStorePhoto(String set, String md5, byte[] content) throws IOException {
		//图片不存在, 只在第一次写的时候连接服务器
		if (syncStoreSocket == null) {
			assert(serverPort > 0);
			syncStoreSocket = new Socket();
			syncStoreSocket.setTcpNoDelay(true);
			syncStoreSocket.connect(new InetSocketAddress(conf.getServerName(), serverPort));
			
			storeos = new DataOutputStream(syncStoreSocket.getOutputStream());
			storeis = new DataInputStream(syncStoreSocket.getInputStream());
		}
		
		//action,set,md5,content的length写过去
		byte[] header = new byte[4];
		header[0] = ActionType.SYNCSTORE;
		header[1] = (byte) set.length();
		header[2] = (byte) md5.length();
		storeos.write(header);
		storeos.writeInt(content.length);
		
		//set,md5,content的实际内容写过去
		storeos.write(set.getBytes());
		storeos.write(md5.getBytes());
		storeos.write(content);
		storeos.flush();
		
		int count = storeis.readInt();
		if (count == -1)
			return jedis.hget(set,md5);
		
		String s = new String(readBytes(count, storeis));
		
		if (s.startsWith("#FAIL:")) {
			throw new IOException("MM server failure: " + s);
		}
		return s;
	}
	
	private void __asyncStorePhoto(String set, String md5, byte[] content) throws IOException {
		//图片不存在, 只在第一次写的时候连接服务器
		if (asyncStoreSocket == null) {
			assert(serverPort > 0);
			asyncStoreSocket = new Socket();
			asyncStoreSocket.setTcpNoDelay(true);
			asyncStoreSocket.connect(new InetSocketAddress(conf.getServerName(), serverPort));
			
			storeos = new DataOutputStream(asyncStoreSocket.getOutputStream());
			storeis = new DataInputStream(asyncStoreSocket.getInputStream());
		}
		
		//action,set,md5,content的length写过去
		byte[] header = new byte[4];
		header[0] = ActionType.ASYNCSTORE;
		header[1] = (byte) set.length();
		header[2] = (byte) md5.length();
		storeos.write(header);
		storeos.writeInt(content.length);
		
		//set,md5,content的实际内容写过去
		storeos.write(set.getBytes());
		storeos.write(md5.getBytes());
		storeos.write(content);
		storeos.flush();
	}
	
	/**
	 * 同步写
	 * @param set
	 * @param md5
	 * @param content
	 * @return		
	 */
	public String syncStorePhoto(String set, String md5, byte[] content) throws IOException {
		if (conf.getMode() == ClientConf.MODE.NODEDUP) {
			return __syncStorePhoto(set, md5, content);
		} else if (conf.getMode() == ClientConf.MODE.DEDUP) {
			String info = jedis.hget(set, md5);
		
			if (info == null) {
				return __syncStorePhoto(set, md5, content);
			} else {
				// NOTE: the delete unit is SET, thus, do NOT need reference 
				//System.out.println(set + "." + md5 + " exists in MM server");
				//jedis.hincrBy(set, "r." + md5, 1);
				
				return info;
			}
		}
		throw new IOException("Invalid Operation Mode.");
	}
	
	public void asyncStorePhoto(String set, String md5, byte[] content)	throws IOException {
		if (conf.getMode() == ClientConf.MODE.NODEDUP) {
			__asyncStorePhoto(set, md5, content);
		} else if (conf.getMode() == ClientConf.MODE.DEDUP) {
			String info = jedis.hget(set, md5);

			if (info == null) {
				__asyncStorePhoto(set, md5, content);
			}
		} else {
			throw new IOException("Invalid Operation Mode.");
		}
	}
	
	public Map<String, String> getNrFromSet(String set) throws IOException {
		return jedis.hgetAll(set);
	}
	
	/**
	 * 
	 * @param set	redis中的键以set开头,因此读取图片要加上它的集合名
	 * @param md5	
	 * @return		图片内容,如果图片不存在则返回长度为0的byte数组
	 */
	public byte[] getPhoto(String set, String md5) throws IOException {
		String info = jedis.hget(set, md5);
		
		if(info == null) {
			System.out.println(set + "@" + md5 + " doesn't exist in redis server.");
			
			return new byte[0];
		} else {
			return searchPhoto(info);
		}
	}
	
	public byte[] searchPhoto(String info) throws IOException {
		String[] infos = info.split("@");
		
		if (infos.length != 8) {
			throw new IOException("Invalid INFO string, info length is " + infos.length);
		}
		
		if (socketHash.containsKey(infos[2] + ":" + infos[3]))
			searchSocket = socketHash.get(infos[2] + ":" + infos[3]);
		else {
			// 读取图片时所用的socket
			searchSocket = new Socket(); 
			searchSocket.connect(new InetSocketAddress(infos[2], Integer.parseInt(infos[3])));
			searchSocket.setTcpNoDelay(true);
			socketHash.put(infos[2] + ":" + infos[3], searchSocket);
		}

		DataInputStream searchis = new DataInputStream(searchSocket.getInputStream());
		DataOutputStream searchos = new DataOutputStream(searchSocket.getOutputStream());

		//action,info的length写过去
		byte[] header = new byte[4];
		header[0] = ActionType.SEARCH;
		header[1] = (byte) info.getBytes().length;
		searchos.write(header);
		
		//info的实际内容写过去
		searchos.write(info.getBytes());
		searchos.flush();

		int count = searchis.readInt();					
		if (count >= 0) {
			return readBytes(count, searchis);
		} else {
			throw new IOException("Internal error in mm server.");
		}
	}
	
	/**
	 * 从输入流中读取count个字节
	 * @param count
	 * @return
	 */
	public byte[] readBytes(int count, InputStream istream) throws IOException
	{

		byte[] buf = new byte[count];			
		int n = 0;
		
		while (count > n) {
			n += istream.read(buf, n, count - n);
		}
		
		return buf;
	}
	
	/**
	 * 关闭流、套接字和与redis的连接
	 * 用于读和写的套接字全部都关闭
	 */
	public void close() {
		try {
			jedis.quit();
			if(storeos != null)
				storeos.close();
			if(storeis != null)
				storeis.close();
			if(syncStoreSocket != null)
				syncStoreSocket.close();
			if(asyncStoreSocket != null)
				asyncStoreSocket.close();
			for (Map.Entry<String, Socket> entry : socketHash.entrySet()) {
				entry.getValue().close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
