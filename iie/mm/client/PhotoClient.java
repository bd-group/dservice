package iie.mm.client;

import iie.mm.client.ClientConf.RedisInstance;
import iie.mm.client.PhotoClient.SocketHashEntry.SEntry;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class PhotoClient {
	private ClientConf conf;
	private RedisFactory rf;
	private static AtomicInteger curgrpno = new AtomicInteger(0);
	private static AtomicInteger curseqno = new AtomicInteger(0);
	private Map<Integer, XRefGroup> wmap = new ConcurrentHashMap<Integer, XRefGroup>();
	
	public RedisFactory getRf() {
		return rf;
	}
	
	public static class XRef {
		int idx;	// orignal index in keys list
		int seqno;
		String key;
		byte[] value = null;
		
		public XRef() {
			seqno = -1;
			key = null;
		}

		public XRef(int idx, String key) {
			this.idx = idx;
			this.key = key;
			this.seqno = curseqno.incrementAndGet();
		}
		
		public String toString() {
			return "ID " + idx + " SEQNO " + seqno + " KEY " + key;
		}
	}
	
	public class XRefGroup {
		private int gid;
		private long bts = 0;
		private AtomicInteger nr = new AtomicInteger(0);
		private ConcurrentHashMap<Integer, XRef> toWait = new ConcurrentHashMap<Integer, XRef>();
		private ConcurrentHashMap<Integer, XRef> fina = new ConcurrentHashMap<Integer, XRef>();
		
		public XRefGroup() {
			gid = curgrpno.incrementAndGet();
			wmap.put(gid, this);
		}
		
		public void addToGroup(XRef xref) {
			bts = System.currentTimeMillis();
			toWait.put(xref.seqno, xref);
			nr.incrementAndGet();
		}
		
		public boolean isTimedout() {
			if (System.currentTimeMillis() - bts >= conf.getMgetTimeout()) {
				return true;
			} else
				return false;
		}
		
		public void doneXRef(Integer seqno, byte[] value) {
			XRef x = toWait.remove(seqno);
			if (x != null) {
				x.value = value;
				fina.put(seqno, x);
			}
		}
		
		public boolean waitAll() {
			return toWait.isEmpty();
		}
		
		public boolean waitAny() {
			return !fina.isEmpty();
		}
		
		public Collection<XRef> getAll() {
			Collection<XRef> tmp = null;
			
			if (fina.size() == nr.get()) {
				tmp = fina.values();
				fina.clear();
			}
			return tmp;
		}
		
		public long getGroupSize() {
			return nr.get();
		}
		
		public long getAvailableSize() {
			return fina.size();
		}
	}
	
	//缓存与服务端的tcp连接,服务端名称到连接的映射
	public static class SocketHashEntry {
		String hostname;
		int port, cnr;
		AtomicInteger xnr = new AtomicInteger(0);
		Map<Long, SEntry> map;
		AtomicLong nextId = new AtomicLong(0);
		
		public static class SEntry {
			public Socket sock;
			public long id;
			public boolean used;
			public DataInputStream dis;
			public DataOutputStream dos;
			
			public SEntry(Socket sock, long id, boolean used, DataInputStream dis, DataOutputStream dos) {
				this.sock = sock;
				this.id = id;
				this.used = used;
				this.dis = dis;
				this.dos = dos;
			}
		}
		
		public SocketHashEntry(String hostname, int port, int cnr) {
			this.hostname = hostname;
			this.port = port;
			this.cnr = cnr;
			this.map = new ConcurrentHashMap<Long, SEntry>();
		}
		
		public void setFreeSocket(long id) {
			SEntry e = map.get(id);
			if (e != null) {
				e.used = false;
			}
			synchronized (this) {
				this.notify();
			}
		}
		
		public boolean probSelected() {
			if (map.size() > 0)
				return true;
			else {
				// 1/100 prob selected
				if (new Random().nextInt(100) == 0)
					return true;
				else 
					return false;
			}
		}
		
		public long getFreeSocket() throws IOException {
			boolean found = false;
			long id = -1;

			do {
				synchronized (this) {
					for (SEntry e : map.values()) {
						if (!e.used) {
							// ok, it is unused
							found = true;
							e.used = true;
							id = e.id;
							break;
						}
					}
				}
	
				if (!found) {
					if (map.size() + xnr.get() < cnr) {
						// do connect now
						Socket socket = new Socket();
						xnr.getAndIncrement();
						try {
							socket.connect(new InetSocketAddress(this.hostname, this.port));
							socket.setTcpNoDelay(true);
							id = this.addToSocketsAsUsed(socket, 
									new DataInputStream(socket.getInputStream()), 
									new DataOutputStream(socket.getOutputStream()));
									//new DataInputStream(new BufferedInputStream(socket.getInputStream())), 
									//new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
							System.out.println("[GFS] New connection @ " + id + " for " + hostname + ":" + port);
						} catch (SocketException e) {
							xnr.getAndDecrement();
							System.out.println("[GFS] Connect to " + hostname + ":" + port + " failed w/ " + e.getMessage());
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e1) {
							}
							throw e;
						} catch (Exception e) {
							xnr.getAndDecrement();
							System.out.println("[GFS] Connect to " + hostname + ":" + port + " failed w/ " + e.getMessage());
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e1) {
							}
							throw new IOException(e.getMessage());
						}
						xnr.getAndDecrement();
					} else {
						do {
							try {
								synchronized (this) {
									//System.out.println("wait ...");
									this.wait(60000);
								}
							} catch (InterruptedException e) {
								e.printStackTrace();
								continue;
							}
							break;
						} while (true);
					}
				} else {
					break;
				}
			} while (id == -1);
			
			return id;
		}
		
		public long addToSocketsAsUsed(Socket sock, DataInputStream dis, DataOutputStream dos) {
			SEntry e = new SEntry(sock, nextId.getAndIncrement(), true, dis, dos);
			synchronized (this) {
				map.put(e.id, e);
			}
			return e.id;
		}
		
		public void addToSockets(Socket sock, DataInputStream dis, DataOutputStream dos) {
			SEntry e = new SEntry(sock, nextId.getAndIncrement(), false, dis, dos);
			synchronized (this) {
				map.put(e.id, e);
			}
		}
		
		public void useSocket(long id) {
			synchronized (this) {
				SEntry e = map.get(id);
				if (e != null) {
					e.used = true;
				}
			}
		}
		
		public void delFromSockets(long id) {
			System.out.println("Del sock @ " + id + " for " + hostname + ":" + port);
			SEntry e = null;
			synchronized (this) {
				e = map.get(id);
				map.remove(id);
				this.notifyAll();
			}
			if (e != null) {
				try {
					e.dis.close();
					e.dos.close();
					e.sock.close();
				} catch (IOException e1) {
				}
			}
		}
		
		public void clear() {
			synchronized (this) {
				for (Map.Entry<Long, SEntry> e : map.entrySet()) {
					try {
						e.getValue().sock.close();
					} catch (IOException e1) {
					}
				}
			}
		}
	};
	
	private Map<String, SocketHashEntry> socketHash = new HashMap<String, SocketHashEntry>();
	private Map<String, SocketHashEntry> igetSH = new HashMap<String, SocketHashEntry>();
	private Map<Long, String> servers = new ConcurrentHashMap<Long, String>();

	public Map<Long, String> getServers() {
		return servers;
	}

	private final ThreadLocal<Jedis> jedis =
         new ThreadLocal<Jedis>() {
             @Override protected Jedis initialValue() {
                 return null;
         }
	};
	
	public PhotoClient(){
		conf = new ClientConf();
		rf = new RedisFactory(conf);
	}

	public PhotoClient(ClientConf conf) {
		this.conf = conf;
		rf = new RedisFactory(conf);
	}
	
	public ClientConf getConf() {
		return conf;
	}
	public void setConf(ClientConf conf) {
		this.conf = conf;
	}
	
	public void addToServers(long id, String server) {
		servers.put(id, server);
	}
	
	public Map<String, SocketHashEntry> getSocketHash() {
		return socketHash;
	}
	public void setSocketHash(Map<String, SocketHashEntry> socketHash) {
		this.socketHash = socketHash;
	}

	public Jedis getJedis() {
		return jedis.get();
	}
	
	public void setJedis(Jedis jedis) {
		this.jedis.set(jedis);
	}
	
	public void refreshJedis() throws IOException {
		synchronized (this) {
			if (jedis.get() == null) {
				RedisInstance ri = conf.getRedisInstance();
				jedis.set(rf.getNewInstance(ri));
			}
		}
		if (jedis.get() == null)
			throw new IOException("Invalid Jedis connection.");
	}
	
	public void recycleJedis() {
		synchronized (this) {
			if (jedis.get() != null) {
				jedis.set(rf.putInstance(jedis.get()));
			}
		}
	}
	
	public List<String> getActiveMMSByHB() throws IOException {
		List<String> ls = new ArrayList<String>();
		int err = 0;

		refreshJedis();
		try {
			Set<String> keys = jedis.get().keys("mm.hb.*");

			for(String hp : keys) {
				String ipport = jedis.get().hget("mm.dns", hp.substring(6));
				if (ipport == null)
					ls.add(hp.substring(6));
				else
					ls.add(ipport);
			}
		} catch (JedisException e) {
			err = -1;
			System.out.println("Get mm.hb.* failed: " + e.getMessage());
		} finally {
			if (err < 0)
				jedis.set(rf.putBrokenInstance(jedis.get()));
			else
				jedis.set(rf.putInstance(jedis.get()));
		}

		return ls;
	}
	
	private byte[] __handleInput(DataInputStream dis) throws IOException {
		int count;
		
		synchronized (dis) {
			count = dis.readInt();
			switch (count) {
			case -1:
				return null;
			default:
				return readBytes(count, dis);
			}
		}
	}
	
	private String __syncStorePhoto(String set, String md5, byte[] content, SocketHashEntry she) throws IOException {
		long id = she.getFreeSocket();
		if (id == -1)
			throw new IOException("Could not find free socket for server: " + she.hostname + ":" + she.port);
		DataOutputStream storeos = she.map.get(id).dos;
		DataInputStream storeis = she.map.get(id).dis;
		
		//action,set,md5,content的length写过去
		byte[] header = new byte[4];
		header[0] = ActionType.SYNCSTORE;
		header[1] = (byte) set.length();
		header[2] = (byte) md5.length();
		
		byte[] r = null;
		
		try {
			synchronized (storeos) {
				storeos.write(header);
				storeos.writeInt(content.length);
				
				//set,md5,content的实际内容写过去
				storeos.write(set.getBytes());
				storeos.write(md5.getBytes());
				storeos.write(content);
				storeos.flush();
			}
			r = __handleInput(storeis);
			she.setFreeSocket(id);
		} catch (Exception e) {
			System.out.println("__syncStore send/recv failed: " + e.getMessage() + " r?null=" + (r == null ? true : false));
			// remove this socket do reconnect?
			she.delFromSockets(id);
		}
		
		if (r == null) {
			// if we can't get reasonable response from redis, report it!
			String rr = null;
			int err = 0;
			
			refreshJedis();
			try {
				rr = jedis.get().hget(set, md5);
			} catch (JedisConnectionException e) {
				System.out.println("Jedis connection broken in __syncStoreObject, wait ...");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
				err = -1;
				jedis.set(rf.putBrokenInstance(jedis.get()));
			} catch (JedisException e) {
				err = -1;
			} finally {
				if (err < 0)
					jedis.set(rf.putBrokenInstance(jedis.get()));
				else
					jedis.set(rf.putInstance(jedis.get()));
			}
			if (rr == null)
				throw new IOException("MM Server failed or Metadata connection broken?");
			return rr;
		}
		
		String s = new String(r, "US-ASCII");
		
		if (s.startsWith("#FAIL:")) {
			throw new IOException("MM server failure: " + s);
		}
		return s;
	}
	
	private void __asyncStorePhoto(String set, String md5, byte[] content, SocketHashEntry she) throws IOException {
		long id = she.getFreeSocket();
		if (id == -1)
			throw new IOException("Could not get free socket for server: " + she.hostname + ":" + she.port);
		DataOutputStream storeos = she.map.get(id).dos;
		
		//action,set,md5,content的length写过去
		byte[] header = new byte[4];
		header[0] = ActionType.ASYNCSTORE;
		header[1] = (byte) set.length();
		header[2] = (byte) md5.length();

		try {
			synchronized (storeos) {
				storeos.write(header);
				storeos.writeInt(content.length);
				
				//set,md5,content的实际内容写过去
				storeos.write(set.getBytes());
				storeos.write(md5.getBytes());
				storeos.write(content);
				storeos.flush();
			}
			she.setFreeSocket(id);
		} catch (Exception e) {
			e.printStackTrace();
			// remove this socket do reconnect?
			she.delFromSockets(id);
		}
	}
	
	/**
	 * 同步写
	 * @param set
	 * @param md5
	 * @param content
	 * @param sock
	 * @return		
	 */
	public String syncStorePhoto(String set, String md5, byte[] content, SocketHashEntry she, boolean nodedup) throws IOException {
		if (conf.getMode() == ClientConf.MODE.NODEDUP || nodedup) {
			return __syncStorePhoto(set, md5, content, she);
		} else if (conf.getMode() == ClientConf.MODE.DEDUP) {
			String info = null;
			int err = 0;
			
			refreshJedis();
			try {
				info = jedis.get().hget(set, md5);
				
				if (info != null) {
					// NOTE: the delete unit is SET, thus, do NOT need reference 
					if (conf.isLogDupInfo())
						jedis.get().hincrBy("mm.dedup.info", set + "@" + md5, 1);

					return info;
				}
			} catch (JedisConnectionException e) {
				System.out.println("Jedis connection broken, wait ...");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
				err = -1;
			} catch (JedisException e) {
				err = -1;
			} finally {
				if (err < 0)
					jedis.set(rf.putBrokenInstance(jedis.get()));
				else
					jedis.set(rf.putInstance(jedis.get()));
			}
			
			return __syncStorePhoto(set, md5, content, she);
		}
		throw new IOException("Invalid Operation Mode.");
	}
	
	public void asyncStorePhoto(String set, String md5, byte[] content, SocketHashEntry she) throws IOException {
		if (conf.getMode() == ClientConf.MODE.NODEDUP) {
			__asyncStorePhoto(set, md5, content, she);
		} else if (conf.getMode() == ClientConf.MODE.DEDUP) {
			String info = null;
			int err = 0;
			
			refreshJedis();
			try {
				info = jedis.get().hget(set, md5);
			} catch (JedisConnectionException e) {
				System.out.println("Jedis connection broken, wait ...");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
				err = -1;
			} catch (JedisException e) {
				err = -1;
			} finally {
				if (err < 0)
					jedis.set(rf.putBrokenInstance(jedis.get()));
				else
					jedis.set(rf.putInstance(jedis.get()));
			}

			if (info == null) {
				__asyncStorePhoto(set, md5, content, she);
			} else { 
				// NOTE: log the dup info
				if (conf.isLogDupInfo())
					jedis.get().hincrBy("mm.dedup.info", set + "@" + md5, 1);
			}
		} else {
			throw new IOException("Invalid Operation Mode.");
		}
	}
	
	//批量存储时没有判断重复
	public String[] mPut(String set, String[] md5s, byte[][] content, SocketHashEntry she) throws IOException {
		long id = she.getFreeSocket();
		if (id == -1)
			throw new IOException("Could not find free socket for server: " + she.hostname + ":" + she.port);
		DataOutputStream dos = she.map.get(id).dos;
		DataInputStream dis = she.map.get(id).dis;
		
		int n = md5s.length;
		String[] r = new String[n];
		byte[] header = new byte[4];
		header[0] = ActionType.MPUT;
		header[1] = (byte) set.length();
		try {
			dos.write(header);
			dos.writeInt(n);
			dos.write(set.getBytes());
			for (int i = 0; i < n; i++) {
				dos.writeInt(md5s[i].getBytes().length);
				dos.write(md5s[i].getBytes());
			}
			for (int i = 0; i < n; i++)
				dos.writeInt(content[i].length);
			for (int i = 0; i < n; i++)
				dos.write(content[i]);
			
			// BUG-XXX: 注意，此处for循环中也还需要处理dis.readInt()的返回值！
			int count = dis.readInt();
			if (count == -1)
				throw new IOException("MM server failure.");
			r[0] = new String(readBytes(count, dis));
			for (int i = 1; i < n; i++)
				r[i] = new String(readBytes(dis.readInt(), dis));
			she.setFreeSocket(id);
		} catch (Exception e) {
			e.printStackTrace();
			// remove this socket do reconnect?
			she.delFromSockets(id);
		}
		return r;
	}
	
	/**
	 * 同步取
	 * @param set	redis中的键以set开头,因此读取图片要加上它的集合名
	 * @param md5	
	 * @return		图片内容,如果图片不存在则返回长度为0的byte数组
	 */
	public byte[] getPhoto(String set, String md5) throws IOException {
		String info = null;
		int err = 0;
		
		refreshJedis();
		try {
			info = jedis.get().hget(set, md5);
		} catch (JedisConnectionException e) {
			System.out.println("Jedis connection broken, wait in getObject ...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
		} catch (JedisException e) {
			err = -1;
		} finally {
			if (err < 0)
				jedis.set(rf.putBrokenInstance(jedis.get()));
			else
				jedis.set(rf.putInstance(jedis.get()));
		}
		
		if (info == null) {
			throw new IOException(set + "@" + md5 + " doesn't exist in MMM server or connection broken.");
		} else {
			return searchPhoto(info);
		}
	}
	
    /**
	 * infos是拼接的元信息，各个元信息用#隔开
	 */
	public byte[] searchPhoto(String infos) throws IOException {
		byte[] r = null;

		for (String info : infos.split("#")) {
			try {
				String[] si = info.split("@");
				
				r = searchByInfo(info, si);
				if (r.length >= 0)
					break;
			} catch(IOException e){
				e.printStackTrace();
				continue;
			}
		}
		if (r == null)
			throw new IOException("Failed to search MM object.");
		return r;
	}
	
	
	/**
	 * info是一个文件的元信息，没有拼接的
	 */
	public byte[] searchByInfo(String info, String[] infos) throws IOException {
		if (infos.length != 7) {
			throw new IOException("Invalid INFO string, info length is " + infos.length);
		}
		
		SocketHashEntry searchSocket = null;
		String server = servers.get(Long.parseLong(infos[2]));
		if (server == null)
			throw new IOException("Server idx " + infos[2] + " can't be resolved.");
		if (socketHash.containsKey(server)) {
			searchSocket = socketHash.get(server);
		} else {
			String[] s = server.split(":");
			if (s.length == 2) {
				Socket socket = new Socket(); 
				socket.connect(new InetSocketAddress(s[0], Integer.parseInt(s[1])));
				socket.setTcpNoDelay(true);
				searchSocket = new SocketHashEntry(s[0], Integer.parseInt(s[1]), conf.getSockPerServer());
				searchSocket.addToSocketsAsUsed(socket,
						new DataInputStream(socket.getInputStream()),
						new DataOutputStream(socket.getOutputStream()));
						//new DataInputStream(new BufferedInputStream(socket.getInputStream())), 
						//new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
						
				socketHash.put(server, searchSocket);
			} else 
				throw new IOException("Invalid server name or port.");
		}

		//action,info的length写过去
		byte[] header = new byte[4];
		header[0] = ActionType.SEARCH;
		header[1] = (byte) info.getBytes().length;
		long id = searchSocket.getFreeSocket();
		if (id == -1)
			throw new IOException("Could not get free socket for server " + server);

		byte[] r = null;
		try {
			searchSocket.map.get(id).dos.write(header);
			
			//info的实际内容写过去
			searchSocket.map.get(id).dos.writeBytes(info);
			searchSocket.map.get(id).dos.flush();
	
			r = __handleInput(searchSocket.map.get(id).dis);
			searchSocket.setFreeSocket(id);
		} catch (Exception e) {
			e.printStackTrace();
			// remove this socket do reconnect?
			searchSocket.delFromSockets(id);
		}
		if (r == null)
			throw new IOException("Internal error in mm server:" + server);
		else
			return r;
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
			for (SocketHashEntry s : socketHash.values()){
				for (SEntry e : s.map.values()) {
					e.sock.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
		
	public Map<String, String> getNrFromSet(String set) throws IOException {
		int err = 0;
		
		refreshJedis();
		try {
			return jedis.get().hgetAll(set);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			System.out.println("Jedis connection broken in getNr, wait ...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
		} catch (JedisException e) {
			err = -1;
		} finally {
			if (err < 0)
				jedis.set(rf.putBrokenInstance(jedis.get()));
			else
				jedis.set(rf.putInstance(jedis.get()));
		}
		throw new IOException("Jedis Connection broken.");
	}
	
	public XRefGroup createXRefGroup() {
		return new XRefGroup();
	}
	
	public void removeXRefGroup(XRefGroup g) {
		wmap.remove(g.gid);
	}
	
	public int __iget(int gid, int seqno, String set, String md5, long alen) throws IOException, StopException {
		String info = null;
		int err = 0;
		
		refreshJedis();
		try {
			info = jedis.get().hget(set, md5);
		} catch (JedisConnectionException e) {
			System.out.println("Jedis connection broken, wait in getObject ...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
		} catch (JedisException e) {
			err = -1;
		} finally {
			if (err < 0)
				jedis.set(rf.putBrokenInstance(jedis.get()));
			else
				jedis.set(rf.putInstance(jedis.get()));
		}
		
		if (info == null) {
			throw new IOException(set + "@" + md5 + " doesn't exist in MMM server or connection broken.");
		} else {
			return __igetInfo(gid, seqno, info, alen);
		}
	}
	
	public int __igetInfo(int gid, int seqno, String infos, long alen) throws IOException, StopException {
		boolean r = false;
		int len = 0;

		for (String info : infos.split("#")) {
			try {
				String[] si = info.split("@");

				len = Integer.parseInt(si[5]);
				alen -= len;
				if (alen <= 0) {
					throw new StopException();
				}
				r = __igetMMObject(gid, seqno, info, si);
				if (r) break;
			} catch (IOException e){
				System.err.println("GID " + gid + " seqno " + seqno + " infos " + infos + " -> Got IOExcpetion : " + e.getMessage());
				continue;
			}
		}
		if (r)
			return len;
		else
			return -1;
	}
	
	public boolean __igetMMObject(int gid, int seqno, String info, String[] infos) throws IOException {
		if (infos.length != 7) {
			throw new IOException("Invalid INFO string, info length is " + infos.length);
		}
		
		SocketHashEntry igetSocket = null;
		String server = servers.get(Long.parseLong(infos[2]));
		if (server == null)
			throw new IOException("Server idx " + infos[2] + " can't be resolved.");
		if (igetSH.containsKey(server)) {
			igetSocket = igetSH.get(server);
		} else {
			String[] s = server.split(":");
			if (s.length == 2) {
				Socket socket = new Socket(); 
				socket.connect(new InetSocketAddress(s[0], Integer.parseInt(s[1])));
				socket.setTcpNoDelay(true);
				igetSocket = new SocketHashEntry(s[0], Integer.parseInt(s[1]), conf.getSockPerServer());
				igetSocket.addToSocketsAsUsed(socket, 
						new DataInputStream(socket.getInputStream()), 
						//new DataInputStream(new BufferedInputStream(socket.getInputStream())), 
						new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
				igetSH.put(server, igetSocket);
			} else 
				throw new IOException("Invalid server name or port.");
		}

		//action,info的length写过去
		byte[] header = new byte[4];
		header[0] = ActionType.IGET;
		header[1] = (byte) info.getBytes().length;
		long id = igetSocket.getFreeSocket();
		if (id == -1)
			throw new IOException("Could not get free socket for server " + server);

		boolean r = true;
		try {
			// do some recv here
			if (igetSocket.map.get(id).dis.available() > 0) {
				try { 
					__doProgress(igetSocket.map.get(id));
				} catch (IOException e1) {
				}
			}
			
			igetSocket.map.get(id).dos.write(header);
			igetSocket.map.get(id).dos.writeInt(gid);
			igetSocket.map.get(id).dos.writeInt(seqno);
			
			//info的实际内容写过去
			igetSocket.map.get(id).dos.writeBytes(info);
			igetSocket.map.get(id).dos.flush();

			// try to get a new socket
			if (igetSocket.xnr.get() < igetSocket.cnr) {
				long xid = igetSocket.getFreeSocket();
				igetSocket.setFreeSocket(xid);
			}
			igetSocket.setFreeSocket(id);
		} catch (Exception e) {
			e.printStackTrace();
			// remove this socket do reconnect?
			try { 
				__doProgress(igetSocket.map.get(id));
			} catch (IOException e1) {
			}
			igetSocket.delFromSockets(id);
			r = false;
		}

		return r;
	}
	
	public long iGet(XRefGroup g, int idx, String set, String md5, long alen) throws IOException, StopException {
		XRef x = new XRef(idx, set + "@" + md5);
		
		// send it to server
		int len = __iget(g.gid, x.seqno, set, md5, alen);
		// put it to group
		if (len >= 0) {
			g.addToGroup(x);
			return x.seqno;
		} else
			throw new IOException("__iget(" + set + "@" + md5 + ") failed.");
	}
	
	public boolean __doProgress(SEntry se) throws IOException {
		int gid;
		int seqno;
		boolean r = false;
		
		synchronized (se) {
			if (se.dis.available() > 0) {
				gid = se.dis.readInt();
				if (gid != -1) {
					seqno = se.dis.readInt();
					int len = se.dis.readInt();
					byte[] b = readBytes(len, se.dis);
					XRefGroup g = wmap.get(gid);
					if (g != null) {
						g.doneXRef(seqno, b);
						r = true;
					}
				}
			}
		}
		return r;
	}
	
	public boolean iWaitAll(XRefGroup g) throws IOException {
		do {
			if (g.waitAll() || g.nr.get() == g.fina.size())
				return true;

			// progress inputs
			for (String server : servers.values()) {
				SocketHashEntry she = igetSH.get(server);
				if (she != null) {
					synchronized (she) {
						for (SEntry se : she.map.values()) {
							try {
								if (__doProgress(se))
									g.bts = System.currentTimeMillis();
							} catch (IOException e1) {
								e1.printStackTrace();
								she.delFromSockets(se.id);
							}
						}
					}
				}
			}
			if (g.isTimedout())
				return false;
		} while (true);
	}
	
	public class StopException extends Exception {
		private static final long serialVersionUID = -7120649613556817964L;
	}
	
	public List<byte[]> mget(List<String> keys, Map<String, String> cookies) throws IOException {
		String bidx_s = cookies.get("idx");
		String alen_s = cookies.get("accept_len");
		int bi = 0, i;
		long alen = 128 * 1024 * 1024;
		
		if (bidx_s != null) 
			bi = Integer.parseInt(bidx_s);
		if (alen_s != null)
			alen = Integer.parseInt(alen_s);
		ArrayList<byte[]> r = new ArrayList<byte[]>(Collections.nCopies(keys.size() - bi, (byte[])null));
		XRefGroup g = createXRefGroup();
		
		// do info get here
		long begin, end;
		begin = System.nanoTime();
		for (i = bi; i < keys.size(); i++) {
			String key = keys.get(i);
			XRef x = new XRef(i, key);
			try {
				for (String info : key.split("#")) {
					try {
						String[] si = info.split("@");

						if (si.length == 7) {
							alen -= Integer.parseInt(si[5]);
							if (alen <= 0) {
								throw new StopException();
							}
							if (__igetMMObject(g.gid, x.seqno, info, si)) {
								g.addToGroup(x);
								break;
							}
						} else {
							int len = __iget(g.gid, x.seqno, si[0], si[1], alen);
							alen -= len;
							if (len >= 0) {
								g.addToGroup(x);
								break;
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
						continue;
					}
				}
			} catch (StopException e) {
				break;
			}
		}
		end = System.nanoTime();
		System.out.println(" -> SEND nr " + (i - bi) + " -> " + ((end - begin) / 1000.0) + " us.");
		cookies.put("idx", i + "");
		
		begin = System.nanoTime();
		if (!iWaitAll(g)) {
			System.out.println("Wait XRefGroup " + g.gid + " timed out: " + g.nr.get() + " " + g.toWait);
		}
		end = System.nanoTime();
		System.out.println(" -> RECV nr " + (i - bi) + " -> " + ((end - begin) / 1000.0) + " us.");
		
		removeXRefGroup(g);
		
		for (XRef x : g.fina.values()) {
			r.set(x.idx - bi, x.value);
		}
		
		return r.subList(0, i - bi);
	}
		
	public TreeSet<Long> getSets(String prefix) throws IOException {
		TreeSet<Long> tranges = new TreeSet<Long>();
		int err = 0;
		refreshJedis();
		
		try {
			Set<String> keys = jedis.get().keys(prefix + "*.srvs");

			if (keys != null && keys.size() > 0) {
				for (String key : keys) {
					key = key.replaceFirst(".srvs", "");
					key = key.replaceFirst(prefix, "");
					try {
						tranges.add(Long.parseLong(key));
					} catch (NumberFormatException e) {
					}
				}
			}
		} catch (JedisException e) {
			err = -1;
		} finally {
			if (err < 0)
				jedis.set(rf.putBrokenInstance(jedis.get()));
			else
				jedis.set(rf.putInstance(jedis.get()));
		}
		
		return tranges;
	}

	public List<String> getSetElements(String set) {
		List<String> r = null;
		int err = 0;
		
		try {
			refreshJedis();
		} catch (IOException e) { 
			return null;
		}
		
		try {
			if (conf.isGetkeys_do_sort()) {
				Map<String, String> kvs = jedis.get().hgetAll(set);
				TreeMap<String, String> t = new TreeMap<String, String>();
				r = new ArrayList<String>();

				for (Map.Entry<String, String> e : kvs.entrySet()) {
					String[] v = e.getValue().split("@|#");
					if (v.length >= 7) {
						t.put(v[6] + "." + v[3] + "." + (String.format("%015d", Long.parseLong(v[4])) + "." + v[2]),
								set + "@" + e.getKey());
					} else {
						r.add(set + "@" + e.getKey());
					}
				}
				r.addAll(t.values());
				/*int i = 0;
				for (Map.Entry<String, String> x : t.entrySet()) {
					if (i > 100)
						break;
					System.out.println(x.getKey() + " -> " + x.getValue());
					i++;
				}*/
				kvs.clear();
				t.clear();
			} else {
				Set<String> t = jedis.get().hkeys(set);
				r = new ArrayList<String>();
				
				for (String v : t) {
					r.add(set + "@" + v);
				}
				t.clear();
			}
		} catch (JedisConnectionException e) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
		} finally {
			if (err < 0)
				jedis.set(rf.putBrokenInstance(jedis.get()));
			else
				jedis.set(rf.putInstance(jedis.get()));
		}
		return r;
	}
	
	/**
	 * getAllSets() will not refresh Jedis connection, caller should do it
	 * 
	 * @return
	 * @throws IOException
	 */
	private TreeSet<String> getAllSets() throws IOException {
		TreeSet<String> tranges = new TreeSet<String>();

		Set<String> keys = jedis.get().keys("*.srvs");

		if (keys != null && keys.size() > 0) {
			for (String key : keys) {
				key = key.replaceAll("\\.srvs", "");
				tranges.add(key);
			}
		}

		return tranges;
	}
	
	private class GenValue {
		boolean isGen = false;
		String newValue = null;
	}
	
	private GenValue __gen_value(String value, Long sid) {
		GenValue r = new GenValue();
		
		for (String info : value.split("#")) {
			String[] si = info.split("@");
			boolean doClean = false;
			
			if (si.length >= 7) {
				try {
					if (Long.parseLong(si[2]) == sid) {
						// ok, we should clean this entry
						doClean = true;
						r.isGen = true;
					}
				} catch (NumberFormatException nfe) {
				}
			}
			if (!doClean) {
				if (r.newValue == null)
					r.newValue = info;
				else
					r.newValue += "#" + info;
			}
		}
		if (!r.isGen)
			return null;
		else
			return r;
	}
	
	public void scrubMetadata(String server, int port) throws IOException {
		String member = server + ":" + port;
		int err = 0;
		
		try {
			refreshJedis();
		} catch (IOException e) {
			System.err.println("refreshJedis() failed w/ " + e.getMessage());
			return;
		}
		
		try {
			Double gd = jedis.get().zscore("mm.active", member);
			if (gd != null) {
				Long sid = gd.longValue();
				TreeSet<String> sets = getAllSets();
				for (String set : sets) {
					System.out.println("-> Begin Scrub SET " + set + " ... ");
					Map<String, String> kvs = jedis.get().hgetAll(set);

					if (kvs != null) {
						for (Map.Entry<String, String> e : kvs.entrySet()) {
							GenValue gv = __gen_value(e.getValue(), sid);
							if (gv != null) {
								// ok, do update now
								if (gv.newValue != null)
									jedis.get().hset(set, e.getKey(), gv.newValue);
								else
									jedis.get().hdel(set, e.getKey());
								System.out.println("HSET " + set + " " + e.getKey() + " " + gv.newValue + " " + e.getValue());
							}
						}
						kvs.clear();
					}
				}
			} else {
				System.out.println("Find server " + member + " failed.");
			}
		} catch (JedisException e) {
			err = -1;
		} finally {
			if (err < 0) 
				jedis.set(rf.putBrokenInstance(jedis.get()));
			else
				jedis.set(rf.putInstance(jedis.get()));
		}
	}
}
