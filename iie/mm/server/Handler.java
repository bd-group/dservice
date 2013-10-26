package iie.mm.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Handler implements Runnable{
	private ServerConf conf;
	private ConcurrentHashMap<String,BlockingQueue<WriteTask>> sq;
	private SocketChannel sc;
	
	private StorePhoto sp;
	
	public Handler(ServerConf conf, SocketChannel sc, ConcurrentHashMap<String,BlockingQueue<WriteTask>> sq) throws IOException {
		this.conf = conf;
		this.sc = sc;
		sc.socket().setTcpNoDelay(true);
		sc.configureBlocking(true);
		this.sq = sq;
		sp = new StorePhoto(conf);
	}
	
	@Override
	public void run() {
		try {
			while(true) {
				ByteBuffer header = ByteBuffer.allocate(4);
				
				if(readBytes(sc, 4, header) == -1) {
					break;
				} else if (header.get(0) == ActionType.STORE) {
					int setlen = header.get(1);
					int md5len = header.get(2);
					ByteBuffer clen = ByteBuffer.allocate(4);
					readBytes(sc, 4, clen);
					int contentlen = clen.getInt(0);
					
					
					//一次把所有的都读出来,减少读取次数
					ByteBuffer setmd5content = ByteBuffer.allocate(setlen + md5len + contentlen);
					readBytes(sc, setlen + md5len + contentlen, setmd5content);
					String set = new String(setmd5content.array(), 0, setlen);
					String md5 = new String(setmd5content.array(), setlen, md5len);
					
					String result = sp.storePhoto(set, md5, setmd5content.array(), setlen + md5len, contentlen);
					// 统计写入的字节数
					ServerProfile.addWrite(contentlen);
					
					//把写的结果返回给客户端
					if (result == null) {
						//说明结果已经在redis里存在
						ByteBuffer r = ByteBuffer.allocate(4);
						r.putInt(-1);
						r.rewind();
						sc.write(r);
					} else {
						ByteBuffer r1 = ByteBuffer.allocate(4);
						ByteBuffer r2 = ByteBuffer.wrap(result.getBytes());
						r1.putInt(result.length());
						r1.rewind();
						r2.rewind();
						sc.write(r1);
						sc.write(r2);
					}
				} else if (header.get(0) == ActionType.SEARCH) {
					long start = System.currentTimeMillis();
					int infolen = header.get(1);

					//每次都有39 40ms延迟
					ByteBuffer infob = ByteBuffer.allocate(infolen);
					readBytes(sc, infolen, infob);
					String info = new String(infob.array());			

					byte[] content = sp.searchPhoto(info);
					// FIXME: ?? 有可能刚刚写进redis的时候，还无法马上读出来,这时候会无法找到图片,返回null
					if (content != null) {
						ByteBuffer r1 = ByteBuffer.allocate(4);
						ByteBuffer r2 = ByteBuffer.wrap(content);
						r1.putInt(content.length);
						sc.write(r1);
						sc.write(r2);
					} else {
						ByteBuffer r = ByteBuffer.allocate(4);
						r.putInt(-1);
						sc.write(r);
					}
					
					ServerProfile.addDelay(System.currentTimeMillis() - start);
				} else if (header.get(0) == ActionType.DELSET) {
					ByteBuffer setb = ByteBuffer.allocate(header.get(1));
					readBytes(sc, header.get(1), setb);
					String set = new String(setb.array());
					BlockingQueue<WriteTask> bq = sq.get(set);
					
					if(bq != null) {
						// 要删除这个集合,把在这个集合上进行写的线程停掉, null作为标志
						bq.add(new WriteTask(null, null, null, 0, 0));
						sq.remove(set);
					}
					sp.delSet(set);
					ByteBuffer r = ByteBuffer.allocate(4);
					r.putInt(1);
					//返回一个字节1,代表删除成功
					sc.write(r);
				}
			}
			if(sp != null)
				sp.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 从输入流中读取count个字节
	 * @param count
	 * @return
	 */
	public int readBytes(SocketChannel sc, int count, ByteBuffer b) {
		int br = 0, bl = count;
		
		while (bl > 0) {
			try {
				br = sc.read(b);
			} catch (IOException e) {
				System.out.println(e);
				return -1;
			}
			if (br < 0)
				break;
			bl -= br;
		}
		return count;
	}
}
