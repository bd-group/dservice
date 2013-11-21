package iie.mm.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.HashMap;
import redis.clients.jedis.Jedis;

public class PhotoClient {
	private ClientConf conf;
	
	//缓存与服务端的tcp连接,服务端名称到连接的映射
	private Map<String, Socket> socketHash = new HashMap<String, Socket>();
	private Map<String, String> servers = new HashMap<String, String>();
	private Jedis jedis = null;
	
	public PhotoClient(){
		conf = new ClientConf();
	}

	public PhotoClient(ClientConf conf) {
		this.conf = conf;
		this.jedis = RedisFactory.getNewInstance(conf.getRedisHost(), conf.getRedisPort());
	}
	
	public ClientConf getConf() {
		return conf;
	}
	public void setConf(ClientConf conf) {
		this.conf = conf;
	}
	
	public void addToServers(long id, String server) {
		servers.put(Long.toString(id), server);
	}
	
	public Map<String, Socket> getSocketHash() {
		return socketHash;
	}
	public void setSocketHash(Map<String, Socket> socketHash) {
		this.socketHash = socketHash;
	}
	
	public Jedis getJedis() {
		return jedis;
	}
	public void setJedis(Jedis jedis) {
		this.jedis = jedis;
	}
	
	private String __syncStorePhoto(String set, String md5, byte[] content, Socket sock) throws IOException {
		
		DataOutputStream storeos = new DataOutputStream(sock.getOutputStream());
		DataInputStream storeis = new DataInputStream(sock.getInputStream());
		
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
	
	private void __asyncStorePhoto(String set, String md5, byte[] content, Socket sock) throws IOException {
		DataOutputStream storeos = new DataOutputStream(sock.getOutputStream());
		
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
	 * @param sock
	 * @return		
	 */
	public String syncStorePhoto(String set, String md5, byte[] content, Socket sock) throws IOException {
		if (conf.getMode() == ClientConf.MODE.NODEDUP) {
			return __syncStorePhoto(set, md5, content,sock);
		} else if (conf.getMode() == ClientConf.MODE.DEDUP) {
			String info = jedis.hget(set, md5);
		
			if (info == null) {
				return __syncStorePhoto(set, md5, content,sock);
			} else {
				// NOTE: the delete unit is SET, thus, do NOT need reference 
				//System.out.println(set + "." + md5 + " exists in MM server");
				//jedis.hincrBy(set, "r." + md5, 1);
				
				return info;
			}
		}
		throw new IOException("Invalid Operation Mode.");
	}
	
	public void asyncStorePhoto(String set, String md5, byte[] content, Socket sock) throws IOException {
		if (conf.getMode() == ClientConf.MODE.NODEDUP) {
			__asyncStorePhoto(set, md5, content,sock);
		} else if (conf.getMode() == ClientConf.MODE.DEDUP) {
			String info = jedis.hget(set, md5);

			if (info == null) {
				__asyncStorePhoto(set, md5, content,sock);
			} /* else { 
				// FIXME: this should increase reference in Server.
				System.out.println(set + "." + md5 + " exists in redis server");
				jedis.hincrBy(set, "r." + md5, 1);

				return info;
			}*/
		} else {
			throw new IOException("Invalid Operation Mode.");
		}
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
	
	/**
	 * infos是拼接的元信息，各个元信息用#隔开
	 */
	public byte[] searchPhoto(String infos) {
		byte[] r = null;
		for (String info : infos.split("#")) {
			try {
				String[] si = info.split("@");
				
				r = searchByInfo(info, si);
				if (r.length > 0)
					break;
			} catch(IOException e){
				e.printStackTrace();
				continue;
			}
		}
		return r;
	}
	
	/**
	 * info是一个文件的元信息，没有拼接的
	 */
	public byte[] searchByInfo(String info, String[] infos) throws IOException {
		if (infos.length != 7) {
			throw new IOException("Invalid INFO string, info length is " + infos.length);
		}
		Socket searchSocket = null;
		String server = servers.get(infos[2]);
		if (socketHash.containsKey(server))
			searchSocket = socketHash.get(server);
		else {
			String[] s = server.split(":");
			if (s.length == 2) {
				searchSocket = new Socket(); 
				searchSocket.connect(new InetSocketAddress(s[0], Integer.parseInt(s[1])));
				searchSocket.setTcpNoDelay(true);
				socketHash.put(server, searchSocket);
			} else 
				throw new IOException("Invalid server name or port.");
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
			throw new IOException("Internal error in mm server:"+searchSocket.getRemoteSocketAddress());
		}
	}
	
	/**
	 * 从输入流中读取count个字节
	 * @param count
	 * @return
	 */
	private byte[] readBytes(int count, InputStream istream) throws IOException {
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
			
			for (Socket s : socketHash.values()){
				s.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
		
	public Map<String, String> getNrFromSet(String set) throws IOException {
		return jedis.hgetAll(set);
	}
}
