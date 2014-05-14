package iie.mm.server;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class StorePhoto {
	private ServerConf conf;
	private String localHostName;
	private int serverport;							//本机监听的端口,在这里的作用就是构造存图片时返回值
	private String destRoot = "./mm_data/";
	private Set<String> storeArray = new HashSet<String>();
	private String[] diskArray;						//代表磁盘的数组
	private long blocksize;							//文件块的大小，单位是B
	private static long writeTo = -1;
	private static long readTo = -1;
	
	//一级的hash,集合 + 磁盘->上下文
	//不能放在构造函数里初始化,不然会每次创建一个storephoto类时,它都被初始化一遍
	private static ConcurrentHashMap<String, StoreSetContext> writeContextHash = new ConcurrentHashMap<String, StoreSetContext>();
	//读文件时的随机访问流，用哈希来缓存
	private static ConcurrentHashMap<String, ReadContext> readRafHash = new ConcurrentHashMap<String, ReadContext>();
	private final ThreadLocal<Jedis> jedis =
         new ThreadLocal<Jedis>() {
             @Override protected Jedis initialValue() {
                 return null;
         }
	};
	private static String sha = null;
	
	private TimeLimitedCacheMap lookupCache = new TimeLimitedCacheMap(10, 60, 300, TimeUnit.SECONDS);
	
	public static class RedirectException extends Exception {
		/**
		 * serialVersionUID
		 */
		private static final long serialVersionUID = 3092261885247728350L;
		public long serverId;
		public String info;
	
		public RedirectException(long serverId, String info) {
			this.serverId = serverId;
			this.info = info;
		}
	}
	
	public static int recycleContextHash() {
		List<String> toDel = new ArrayList<String>();
		int nr = 0;
		
		for (Map.Entry<String, StoreSetContext> entry : writeContextHash.entrySet()) {
			if (entry.getValue().openTs > 0 && 
					System.currentTimeMillis() - entry.getValue().openTs > writeTo) {
				toDel.add(entry.getKey());
			}
		}
		for (String key : toDel) {
			StoreSetContext ssc = writeContextHash.get(key);
			if (ssc != null) {
				synchronized (ssc) {
					if (ssc.raf != null)
						try {
							ssc.raf.close();
							ssc.raf = null;
							nr++;
						} catch (IOException e) {
							e.printStackTrace();
						}
				}
			}
		}
		
		return nr;
	}
	
	public static int recycleRafHash() {
		List<String> toDel = new ArrayList<String>();
		int nr = 0;
		
		for (Map.Entry<String, ReadContext> entry : readRafHash.entrySet()) {
			if (entry.getValue().accessTs > 0 && 
					System.currentTimeMillis() - entry.getValue().accessTs > readTo) {
				toDel.add(entry.getKey());
			}
		}
		for (String key : toDel) {
			ReadContext rc = readRafHash.get(key);
			if (rc != null) {
				try {
					nr += rc.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return nr;
	}
	
	public class ReadContext {
		public long accessTs = -1;
		public RandomAccessFile raf = null;
		public AtomicInteger ref = new AtomicInteger(0);
		public String name, mode;
		
		public ReadContext(String name, String mode) throws FileNotFoundException {
			raf = new RandomAccessFile(name, mode);
			this.name = name;
			this.mode = mode;
		}
		
		public void updateAccessTs() {
			accessTs = System.currentTimeMillis();
		}
		
		public int close() throws IOException {
			synchronized (this) {
				if (raf != null) {
					raf.close();
					raf = null;
					return 1;
				}
			}
			return 0;
		}
		
		public void reopen() throws IOException {
			synchronized (this) {
				if (raf == null) {
					raf = new RandomAccessFile(name, mode);
				}
			}
		}
	}

	public class StoreSetContext {
		public String key;
		public String disk;
		
		public long openTs = -1;
		//当前可写的块
		private long curBlock = -1;
		private long offset = 0;
		//代表要写的块的文件
		private File newf = null;
		//写当前的块的随机访问流
		private RandomAccessFile raf = null;
		
		private String path = null;
		
		public StoreSetContext(String set, String disk) {
			// 根据set和md5构造存储的路径
			StringBuffer sb = new StringBuffer();
			sb.append(disk);
			sb.append("/");
			sb.append(destRoot);
			sb.append(set);
			sb.append("/");
			path = sb.toString();
			//存储文件的文件夹的相对路径，不包含文件名
			File dir = new File(path);
			if (!dir.exists()) 
				dir.mkdirs();
			
			this.key = set + ":" + disk;
			this.disk = disk;
		}
	}
	
	public StorePhoto(ServerConf conf) {
		this.conf = conf;
		serverport = conf.getServerPort();
		blocksize = conf.getBlockSize();
		storeArray = conf.getStoreArray();
		if (storeArray.size() == 0) {
			storeArray.add(".");
		}
		diskArray = storeArray.toArray(new String[0]);
		//jedis = new RedisFactory(conf).getDefaultInstance();

		localHostName = conf.getNodeName();
		writeTo = conf.getWrite_fd_recycle_to();
		readTo = conf.getRead_fd_recycle_to();
	}
	
	public void reconnectJedis() throws IOException {
		if (jedis.get() == null) {
			jedis.set(new RedisFactory(conf).getDefaultInstance());
		}
		if (jedis.get() == null)
			throw new IOException("Connection to MMM Server failed.");
	}

	/**
	 * 把content代表的图片内容,存储起来,把小图片合并成一个块,块大小由配置文件中blocksize指定.
	 * 文件存储在destRoot下，然后按照set分第一层子目录
	 * @param set	集合名
	 * @param md5	文件的md5
	 * @param content	文件内容
	 * @return		type@set@serverid@block@offset@length@disk,这几个信息通过redis存储,分别表示元信息类型,该图片所属集合,所在节点,
	 * 				节点的端口号,所在相对路径（包括完整文件名）,位于所在块的偏移的字节数，该图片的字节数,磁盘
	 */
	public String storePhoto(String set, String md5, byte[] content, int coff, 
			int clen) {
		String returnStr = "#FAIL: unknown error.";
		int err = 0;
		
		try {
			reconnectJedis();
		} catch (IOException e2) {
			return "#FAIL: MMM Server can not be reached.";
		}
		if (sha == null) {
			String script = "local temp = redis.call('hget', KEYS[1], ARGV[1]);"
					+ "if temp then "
					+ "temp = temp..\"#\"..ARGV[2] ;"
					+ "redis.call('hset',KEYS[1],ARGV[1],temp);"
					+ "return temp;"
					+ "else "
					+ "redis.call('hset',KEYS[1],ARGV[1],ARGV[2]);"
					+ "return ARGV[2] end";
			sha = jedis.get().scriptLoad(script);
//			System.out.println(sha);
		}
		
		StringBuffer rVal = new StringBuffer(128);
		
		//随机选一个磁盘
		int diskid = new Random().nextInt(diskArray.length);
		StoreSetContext ssc = null;

		do {
			ssc = writeContextHash.get(set + ":" + diskArray[diskid]);
			if (ssc != null)
				break;
			ssc = new StoreSetContext(set, diskArray[diskid]);
			ssc = writeContextHash.putIfAbsent(ssc.key, ssc);
		} while (ssc == null);
		
		synchronized (ssc) {
			//找到当前可写的文件块,如果当前不够大,或不存在,则新创建一个,命名block＿id,id递增,redis中只存储id
			//用curBlock缓存当前可写的块，减少查询jedis的次数
		
			try {
				if (ssc.curBlock < 0) {
					//需要通过节点名字来标示不同节点上相同名字的集合
					String reply = jedis.get().get(set + ".blk." + localHostName + "." + ssc.disk);
					if (reply != null) {
						ssc.curBlock = Long.parseLong(reply);
						ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
						ssc.offset = ssc.newf.length();
					} else {
						ssc.curBlock = 0;
						ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
						//把集合和它所在节点记录在redis的set里,方便删除,set.srvs表示set所在的服务器的位置
						jedis.get().sadd(set + ".srvs", localHostName + ":" + serverport);
						jedis.get().set(set + ".blk." + localHostName + "." + ssc.disk, "" + ssc.curBlock);
						ssc.offset = 0;
					}
					ssc.raf = new RandomAccessFile(ssc.newf, "rw");
					ssc.openTs = System.currentTimeMillis();
					ssc.raf.seek(ssc.offset);
				}
				if (ssc.offset + content.length > blocksize) {
					ssc.curBlock++;
					ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
					//如果换了一个新块,则先把之前的关掉
					if (ssc.raf != null)
						ssc.raf.close();
					//当前可写的块号加一
					jedis.get().incr(set + ".blk." + localHostName + "." + ssc.disk);
					ssc.offset = 0;
					ssc.raf = new RandomAccessFile(ssc.newf, "rw");
					ssc.openTs = System.currentTimeMillis();
				}
				
				//在每个文件前面写入它的md5和offset length，从而恢复元数据
				//md5 32个字节，offset:length分配20个字节
//				ssc.offset += 52;
				// 统计写入的字节数
				ServerProfile.addWrite(clen);
				// 构造返回值
				rVal.append("1@"); // type
				rVal.append(set);
				rVal.append("@");
				rVal.append(ServerConf.serverId);
				rVal.append("@");
				rVal.append(ssc.curBlock);
				rVal.append("@");
				rVal.append(ssc.offset);
				rVal.append("@");
				rVal.append(clen);
				rVal.append("@");
				//磁盘,现在存的是磁盘的名字,读取的时候直接拿来构造路径
				rVal.append(diskArray[diskid]);
				
				if (ssc.raf == null) {
					ssc.raf = new RandomAccessFile(ssc.newf, "rw");
					ssc.openTs = System.currentTimeMillis();
					ssc.raf.seek(ssc.offset);
				}
				ssc.raf.write(content, coff, clen);
	
				ssc.offset += clen;
			} catch (JedisConnectionException e) {
				System.out.println("Jedis connection broken in storeObject.");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
				err = -1;
				returnStr = "#FAIL:" + e.getMessage();
			} catch (JedisException e) {
				e.printStackTrace();
				err = -1;
				returnStr = "#FAIL:" + e.getMessage();
			} catch (Exception e) {
				e.printStackTrace();
				err = -1;
				returnStr = "#FAIL:" + e.getMessage();
			} finally {
				if (err < 0) {
					jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
					ServerProfile.writeErr.incrementAndGet();
					return returnStr;
				}
			}
		}
		
		try {
			returnStr = rVal.toString();
			returnStr = jedis.get().evalsha(sha, 1, set, md5, returnStr).toString();
			for (String feature : conf.getFeatures()) {
				ImageMatch.add(new ImageMatch.ImgKeyEntry(feature, content, coff, clen, set, md5));
			}
		} catch (JedisConnectionException e) {
			System.out.println("Jedis connection broken in storeObject.");
			e.printStackTrace();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
			returnStr = "#FAIL:" + e.getMessage();
		} catch (JedisException e) {
			e.printStackTrace();
			err = -1;
			if (e.getMessage().startsWith("NOSCRIPT"))
				sha = null;
			returnStr = "#FAIL:" + e.getMessage();
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
			returnStr = "#FAIL:" + e.getMessage();
		} finally {
			if (err < 0) {
				jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
				ServerProfile.writeErr.incrementAndGet();
			} else
				jedis.set(RedisFactory.putInstance(jedis.get()));
		}
		return returnStr;
	}
	
	/**
	 * 
	 * @param set
	 * @param md5
	 * @param content
	 * @return 出现任何错误返回null，出现错误的话不知道哪些存储成功，哪些不成功
	 */
	public String[] mstorePhoto(String set, String[] md5, byte[][] content) {
		int err = 0;
		
		try {
			reconnectJedis();
		} catch (IOException e2) {
			e2.printStackTrace();
			return null;
		}

		if (!(md5.length == content.length && md5.length == content.length)) {
			System.out.println("Array lengths in arguments mismatch.");
			return null;
		}
		if (sha == null) {
			String script = "local temp = redis.call('hget', KEYS[1], ARGV[1]);"
					+ "if temp then "
					+ "temp = temp..\"#\"..ARGV[2];"
					+ "redis.call('hset',KEYS[1],ARGV[1],temp);"
					+ "return temp;"
					+ "else "
					+ "redis.call('hset',KEYS[1],ARGV[1],ARGV[2]);"
					+ "return ARGV[2] end";
			sha = jedis.get().scriptLoad(script);
//			System.out.println(sha);
		}
		
		String[] returnVal = new String[content.length];
		int diskid = new Random().nextInt(diskArray.length);
		StoreSetContext ssc = null;
		
		do {
			ssc = writeContextHash.get(set + ":" + diskArray[diskid]);
			if (ssc != null)
				break;
			ssc = new StoreSetContext(set, diskArray[diskid]);
			ssc = writeContextHash.putIfAbsent(ssc.key, ssc);
		} while (ssc == null);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		long init_offset = 0;
		
		synchronized (ssc) {
			for (int i = 0; i < content.length; i++) {
				try {
					reconnectJedis();
				} catch (IOException e2) {
					e2.printStackTrace();
					return null;
				}
				
				StringBuffer rVal = new StringBuffer(128);

				try {
					if (ssc.curBlock < 0) {
						// 需要通过节点名字来标示不同节点上相同名字的集合
						String reply = jedis.get().get(set + ".blk." + localHostName
								+ "." + ssc.disk);
						if (reply != null) {
							ssc.curBlock = Long.parseLong(reply);
							ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
							ssc.offset = ssc.newf.length();
						} else {
							ssc.curBlock = 0;
							ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
							// 把集合和它所在节点记录在redis的set里,方便删除,set.srvs表示set所在的服务器的位置
							jedis.get().sadd(set + ".srvs", localHostName + ":"
									+ serverport);
							jedis.get().set(set + ".blk." + localHostName + "."
									+ ssc.disk, "" + ssc.curBlock);
							ssc.offset = 0;
						}
						ssc.raf = new RandomAccessFile(ssc.newf, "rw");
						ssc.openTs = System.currentTimeMillis();
						ssc.raf.seek(ssc.offset);
					}

					if (ssc.offset + content[i].length > blocksize) {
						ssc.curBlock++;
						ssc.newf = new File(ssc.path + "b" + ssc.curBlock);
						// 如果换了一个新块,先把之前的写进去，然后再关闭流
						if (ssc.raf != null) {
							ssc.raf.write(baos.toByteArray());
							ssc.raf.close();
							baos.reset();
						}
						// 当前可写的块号加一
						jedis.get().incr(set + ".blk." + localHostName + "."
								+ ssc.disk);
						ssc.offset = 0;
						ssc.raf = new RandomAccessFile(ssc.newf, "rw");
						ssc.openTs = System.currentTimeMillis();
					}

					// 在每个文件前面写入它的md5和offset length，从而恢复元数据
					// md5 32个字节，offset:length分配20个字节
					// ssc.offset += 52;
					// 统计写入的字节数
					ServerProfile.addWrite(content[i].length);
					// 构造返回值
					rVal.append("1@"); //type
					rVal.append(set);
					rVal.append("@");
					rVal.append(ServerConf.serverId);
					rVal.append("@");
					rVal.append(ssc.curBlock);
					rVal.append("@");
					rVal.append(ssc.offset);
					rVal.append("@");
					rVal.append(content[i].length);
					rVal.append("@");
					// 磁盘,现在存的是磁盘的名字,读取的时候直接拿来构造路径
					rVal.append(diskArray[diskid]);

					baos.write(content[i]);
					returnVal[i] = rVal.toString();
					if (i == 0)
						init_offset = ssc.offset;
					ssc.offset += content[i].length;
				} catch (JedisConnectionException e) {
					System.out.println("Jedis connection broken in mstoreObject.");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
					}
					err = -1;
				} catch (JedisException e) {
					e.printStackTrace();
					err = -1;
				} catch (Exception e) {
					e.printStackTrace();
					err = -1;
				} finally {
					if (err < 0) {
						jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
						ServerProfile.writeErr.incrementAndGet();
						return null;
					}
				}
			}
			// do write now
			try {
				if (ssc.raf == null) {
					ssc.raf = new RandomAccessFile(ssc.newf, "rw");
					ssc.openTs = System.currentTimeMillis();
					ssc.raf.seek(ssc.offset);
				}
				ssc.raf.write(baos.toByteArray());
			} catch (IOException e) {
				e.printStackTrace();
				// Rollback SSC offset
				ssc.offset = init_offset;
				jedis.set(RedisFactory.putInstance(jedis.get()));
				ServerProfile.writeErr.incrementAndGet();
				return null;
			} 
		}
		
		try {
			for (int i = 0; i < content.length; i++) {
				returnVal[i] = jedis.get().evalsha(sha, 1, set, md5[i], returnVal[i]).toString();
				for (String feature : conf.getFeatures()) {
					ImageMatch.add(new ImageMatch.ImgKeyEntry(feature, content[i], 0, content[i].length, set, md5[i]));
				}
			}
		} catch (JedisConnectionException e) {
			System.out.println("Jedis connection broken in mstoreObject.");
			e.printStackTrace();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
			returnVal = null;
		} catch (JedisException e) {
			e.printStackTrace();
			err = -1;
			if (e.getMessage().startsWith("NOSCRIPT"))
				sha = null;
			returnVal = null;
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
			returnVal = null;
		} finally {
			if (err < 0) {
				jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
				ServerProfile.writeErr.incrementAndGet();
			} else
				jedis.set(RedisFactory.putInstance(jedis.get()));
		}
		return returnVal;
	}
	
	/**
	 *获得md5值所代表的图片的内容
	 * @param md5		与storePhoto中的参数md5相对应
	 * @return			该图片的内容,与storePhoto中的参数content对应
	 */
	public byte[] getPhoto(String set, String md5) throws RedirectException {
		try {
			reconnectJedis();
		} catch (IOException e2) {
			return null;
		}
		String info = null;
		int err = 0;
		
		// Step 1: check the local lookup cache
		info = (String) lookupCache.get(set + "." + md5);
		if (info == null) {
			try {
				info = jedis.get().hget(set, md5);
			} catch (JedisConnectionException e) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
				err = -1;
				return null;
			} catch (JedisException e) {
				err = -1;
				return null;
			} finally {
				if (err < 0) {
					jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
					ServerProfile.readErr.incrementAndGet();
				} else
					jedis.set(RedisFactory.putInstance(jedis.get()));
			}
			
			if (info == null) {
				System.out.println("MM: md5:" + md5 + " doesn't exist in set:" + set + ".");
				return null;
			} else {
				// append this entry to lookup cache
				lookupCache.put(set + "." + md5, info);
			}
		}
		// split if it is complex uri
		String savedInfo = null;
		Long savedId = -1L;
		for (String i : info.split("#")) {
			String[] is = i.split("@");
			
			if (Long.parseLong(is[2]) == ServerConf.serverId)
				return searchPhoto(i, is);
			else {
				savedInfo = i;
				savedId = Long.parseLong(is[2]);
			}
		}

		throw new RedirectException(savedId, savedInfo);
	}
	/**
	 * 获得图片内容
	 * @param info		对应storePhoto的type@set@serverid@block@offset@length@disk格式的返回值
	 * @return			图片内容content
	 */
	public byte[] searchPhoto(String info, String[] infos) throws RedirectException {
		long start = System.currentTimeMillis();
		if (infos == null)
			infos = info.split("@");
		
		if (infos.length != 7) {
			System.out.println("Invalid INFO string: " + info);
			return null;
		}
		if (Long.parseLong(infos[2]) != ServerConf.serverId) {
			// this request should be send to another server
			throw new RedirectException(Long.parseLong(infos[2]), info);
		}
		String path = infos[6] + "/" + destRoot + infos[1] + "/b" + infos[3];
		ReadContext readr = null;
		byte[] content = new byte[Integer.parseInt(infos[5])];
	
		try {
			//用哈希缓存打开的文件随机访问流
			do {
				readr = readRafHash.get(path);
				if (readr != null)
					break;
				//构造路径时加上磁盘 
				ReadContext nreadr = new ReadContext(path, "r");
				readr = readRafHash.putIfAbsent(path, nreadr);
				if (readr != null) {
					nreadr.close();
				}
			} while (readr == null);
			
			synchronized (readr) {
				if (readr.raf == null)
					readr.reopen();
				readr.raf.seek(Long.parseLong(infos[4]));
				readr.raf.read(content);
				readr.updateAccessTs();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (NumberFormatException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			readRafHash.remove(path);
			return null;
		}
		ServerProfile.updateRead(content.length, System.currentTimeMillis() - start);

		return content;
	}
	
	public void delSet(String set) {
		for (String d : diskArray)		//删除每个磁盘上的该集合
			delFile(new File(d + "/" + destRoot + set));
		//删除一个集合后,同时删除关于该集合的全局的上下文
		for(String d : diskArray)
			writeContextHash.remove(set+ ":" + d);			
	}
	
	/**
	 * 删除文件.如果是一个文件,直接删除,如果是文件夹,递归删除子文件夹和文件
	 * @param f
	 */
	private void delFile(File f) {

		if(!f.exists())
			return;
		if(f.isFile())
			f.delete();
		else {
			for (File a : f.listFiles())
				if (a.isFile())
					a.delete();
				else
					delFile(a);
			f.delete();
		}
	}
	
	public Set<String> getSetElements(String set) {
		Set<String> r = null;
		int err = 0;
		
		try {
			reconnectJedis();
		} catch (IOException e) { 
			return null;
		}
		
		try {
			r = jedis.get().hkeys(set);
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
				jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
			else
				jedis.set(RedisFactory.putInstance(jedis.get()));
		}
		return r;
	}
	
	public static class SetStats {
		public long rnr; // record nr
		public long fnr; // file nr
		
		public SetStats(long rnr, long fnr) {
			this.rnr = rnr;
			this.fnr = fnr;
		}
	}
	
	/**
	 * 获得redis中每个set的块数，存在hash表里，键是[集合名，该集合内的文件数]，值是块数
	 * @return
	 */
	public TreeMap<String, SetStats> getSetBlks() {
		int err = 0;
		
		try {
			reconnectJedis();
		} catch (IOException e2) {
			return null;
		}
		TreeMap<String, SetStats> temp = new TreeMap<String, SetStats>();
		try {
			Set<String> keys = jedis.get().keys("*.blk.*");
			
			if (keys != null && keys.size() > 0) {
				String[] keya = keys.toArray(new String[0]);
				List<String> vals = jedis.get().mget(keya);
				
				for (int i = 0; i < keya.length; i++) {
					String set = keya[i].split("\\.")[0];
					temp.put(set, new SetStats(jedis.get().hlen(set), temp.containsKey(set) ? temp.get(set).fnr + Integer.parseInt(vals.get(i)) + 1 : Integer.parseInt(vals.get(i)) + 1 ));
				}
			}
		} catch (JedisConnectionException e) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
			temp = null;
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
			temp = null;
		} finally {
			if (err < 0)
				jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
			else
				jedis.set(RedisFactory.putInstance(jedis.get()));
		}
		
		return temp;
	}
	
	public Map<String,String> getDedupInfo() {
		int err = 0;

		try {
			reconnectJedis();
		} catch (IOException e2) {
			e2.printStackTrace();
			return null;
		}
		try {
			Map<String, String> di = jedis.get().hgetAll("mm.dedup.info");
			return di;
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			err = -1;
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
		} finally {
			if (err < 0)
				jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
			else
				jedis.set(RedisFactory.putInstance(jedis.get()));
		}

		return null;
	}

	public String getClientConfig(String field) {
		int err = 0;

		try {
			reconnectJedis();
		} catch (IOException e2) {
			e2.printStackTrace();
			return null;
		}
		try {
			String di = jedis.get().hget("mm.client.conf", field);
			return di;
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			err = -1;
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
		} finally {
			if (err < 0)
				jedis.set(RedisFactory.putBrokenInstance(jedis.get()));
			else
				jedis.set(RedisFactory.putInstance(jedis.get()));
		}

		return null;
	}

	//关闭jedis连接,关闭文件访问流
	public void close() {
		try {
		} finally {
			if (jedis.get() != null)
				jedis.set(RedisFactory.putInstance(jedis.get()));
		}
	}
	
	/**
	 * 
	 * @param feature
	 * @param d
	 * @return
	 * @throws IOException 
	 */
	public List<String> imageMatch(BufferedImage bi, int d) throws IOException {
		List<String> r = new ArrayList<String>();
		
		for (String feature : conf.getFeatures()) {
			if (feature.equalsIgnoreCase(ServerConf.FeatureType.PHASH_IMAGE_ES)) {
				String hc = new ImagePHash().getHash(bi);
				r.addAll(FeatureIndex.getObject(hc, feature, d));
			}
		}
		
		return r;
	}

}
