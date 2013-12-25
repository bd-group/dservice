package iie.mm.server;

import iie.mm.server.StorePhoto.RedirectException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.hyperic.sigar.SigarException;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class Handler implements Runnable{
	private ServerConf conf;
	private ConcurrentHashMap<String,BlockingQueue<WriteTask>> sq;
	private Socket s;
	
	private StorePhoto sp;
	
	private DataInputStream dis;
	private DataOutputStream dos;					//向客户端的输出流
	
	public Handler(ServerConf conf, Socket s, ConcurrentHashMap<String, BlockingQueue<WriteTask>> sq) throws IOException {
		this.conf = conf;
		this.s = s;
		s.setTcpNoDelay(true);
		this.sq = sq;
		dis = new DataInputStream(this.s.getInputStream());
		dos = new DataOutputStream(this.s.getOutputStream());
		sp = new StorePhoto(conf);
	}
	
	@Override
	public void run() {
		try {
			while (true) {
				byte[] header = new byte[4];
				
				dis.readFully(header);
				if (header[0] == ActionType.SYNCSTORE) {
					int setlen = header[1];
					int md5len = header[2];
					int contentlen = dis.readInt();
					
					//一次把所有的都读出来,减少读取次数
					byte[] setmd5content = readBytes(setlen + md5len + contentlen, dis);
					String set = new String(setmd5content, 0, setlen);
					String md5 = new String(setmd5content, setlen, md5len);
					
					String result = null;
					try {
						result = sp.storePhoto(set, md5, setmd5content, setlen + md5len, contentlen);
					} catch (JedisException e) {
						result = "#FAIL:" + e.getMessage();
					}

					if (result == null)
						dos.writeInt(-1);
					else {
						dos.writeInt(result.getBytes().length);
						dos.write(result.getBytes());
					}
					dos.flush();
				} else if(header[0] == ActionType.ASYNCSTORE){
					int setlen = header[1];
					int md5len = header[2];
					int contentlen = dis.readInt();
					
					//一次把所有的都读出来,减少读取次数
					byte[] setmd5content = readBytes(setlen + md5len + contentlen, dis);
					String set = new String(setmd5content, 0, setlen);
					String md5 = new String(setmd5content, setlen, md5len);
					
					WriteTask t = new WriteTask(set, md5, setmd5content, setlen + md5len, contentlen);
					BlockingQueue<WriteTask> bq = sq.get(set);

					if (bq != null)	{
						//存在这个键,表明该写线程已经存在,直接把任务加到任务队列里即可
						bq.add(t);
					} else {
						//如果不存在这个键,则需要新开启一个写线程
						BlockingQueue<WriteTask> tasks = new LinkedBlockingQueue<WriteTask>();
						tasks.add(t);
						sq.put(set, tasks);
						WriteThread wt = new WriteThread(conf,set, sq);
						new Thread(wt).start();
					}
				} else if(header[0] == ActionType.MPUT){
					int setlen = header[1];
					int n = dis.readInt();		
					String set = new String(readBytes(setlen, dis));
					String[] md5s = new String[n];
					int[] conlen = new int[n];
					byte[][] content = new byte[n][];
					for(int i = 0; i<n;i++)
					{
						md5s[i] = new String(readBytes(dis.readInt(), dis)); 
					}
					for(int i = 0;i<n;i++)
						conlen[i] = dis.readInt();
					for(int i = 0;i<n;i++)
					{
						content[i] = readBytes(conlen[i], dis);
//						System.out.println("in handler"+content[i].length);
					}
					
					String[] r = sp.mstorePhoto(set,md5s,content);
					if(r == null)
						dos.writeInt(-1);
					else
					{
						for(int i = 0;i<n;i++)
						{
							dos.writeInt(r[i].getBytes().length);
							dos.write(r[i].getBytes());
						}
					}
					dos.flush();
				} else if (header[0] == ActionType.SEARCH) {
					//这样能把byte当成无符号的用，拼接的元信息长度最大可以255
					int infolen = header[1]&0xff;		

					if (infolen > 0) {
						String infos = new String(readBytes(infolen, dis));		
						byte[] content = null;
						try {
							content = sp.searchPhoto(infos, null);
						} catch (RedirectException e) {
						}
						// FIXME: ?? 有可能刚刚写进redis的时候，还无法马上读出来,这时候会无法找到图片,返回null
						if (content != null) {
							dos.writeInt(content.length);
							dos.write(content);
						} else {
							dos.writeInt(-1);
						}
					} else {
						dos.writeInt(-1);
					}
					dos.flush();
				} else if(header[0] == ActionType.IGET){
					int infolen = header[1]&0xff;
					long id = dis.readLong();
					if(infolen > 0)
					{
						String info = new String( readBytes(infolen, dis));
						byte[] content = null;
						try {
							content = sp.searchPhoto(info, null);
						} catch (RedirectException e) {
						}
						if (content != null) {
							dos.writeLong(id);
							dos.writeInt(content.length);
							dos.write(content);
						} else {
							dos.writeLong(-1);
						}
					}
					else {
						dos.writeLong(-1);
					}
				} else if (header[0] == ActionType.DELSET) {
					String set = new String(readBytes(header[1], dis));

					BlockingQueue<WriteTask> bq = sq.get(set);
					
					if(bq != null) {
						// 要删除这个集合,把在这个集合上进行写的线程停掉, null作为标志
						bq.add(new WriteTask(null, null, null, 0, 0));
						
					}
					sp.delSet(set);
					dos.write(1);			//返回一个字节1,代表删除成功
					dos.flush();
				} else if(header[0] == ActionType.SERVERINFO) {
					ServerInfo si = new ServerInfo();
					String str = "";
					try {
						str += si.getCpuTotalInfo() + System.getProperty("line.separator");
						str += si.getMemInfo()+ System.getProperty("line.separator");
						for(String s : si.getDiskInfo())
							str += s+ System.getProperty("line.separator");
					} catch (SigarException e) {
						str = "#FAIL:" + e.getMessage();
						e.printStackTrace();
					}
					
					dos.writeInt(str.length());
					dos.write(str.getBytes());
					dos.flush();
				}
			}
		} catch (EOFException e) {
			// socket close, it is ok
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sp != null)
				sp.close();
			try {
				s.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 从输入流中读取count个字节
	 * @param count
	 * @return
	 */
	public byte[] readBytes(int count, InputStream istream) throws IOException {
		byte[] buf = new byte[count];			
		int n = 0;

		while (count > n) {
			n += istream.read(buf, n, count - n);
		}
		
		return buf;
	}
}
