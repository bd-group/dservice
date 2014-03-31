package iie.mm.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class DeleteSet {

	private String redisHost;
	private int redisPort;
	private Jedis jedis;
	
	public DeleteSet(String redisHost, int redisPort) {
		this.redisHost = redisHost;
		this.redisPort = redisPort;
		jedis = RedisFactory.getRawInstance(redisHost, redisPort);
	}

	public void delSet(String set) {
		// 向所有拥有该集合的server发送删除请求,删除节点上的文件
		Iterator<String> ir = jedis.smembers(set + ".srvs").iterator();
		String info;
		while (ir.hasNext()) {
			info = ir.next();
			String[] infos = info.split(":");
			if(infos.length != 2) {
				System.out.println("invalid format addr:" + info);
				continue;
			}
			Socket s = new Socket();
			try {
				s.setSoTimeout(60000);
				s.connect(new InetSocketAddress(infos[0], Integer.parseInt(infos[1])));
				byte[] header = new byte[4];
				header[0] = ActionType.DELSET;
				header[1] = (byte) set.length();
				OutputStream os = s.getOutputStream();
				InputStream is = s.getInputStream();
				os.write(header);
				os.write(set.getBytes());
				if (is.read() != 1)
					System.out.println(s.getRemoteSocketAddress() + "，删除时出现异常");
				else
					System.out.println(s.getRemoteSocketAddress() + "，删除成功");
				s.close();
			} catch (SocketTimeoutException e) {
				e.printStackTrace();
				System.out.println(s.getRemoteSocketAddress() + "无响应");
				return;
			} catch (ConnectException e) {
				e.printStackTrace();
				System.out.println("删除出现错误: " + infos[0] + " 拒绝连接");
				return;
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		}

		// 删除集合所在节点,集合对应块号,都在数据库0
		Iterator<String> ikeys1 = jedis.keys(set + ".*").iterator();
		if (!ikeys1.hasNext()) {
			System.out.println("没有这个集合: " + set);
			return;
		}
		Pipeline pipeline1 = jedis.pipelined();
		while (ikeys1.hasNext()) {
			String key1 = ikeys1.next();
			pipeline1.del(key1);
		}
		pipeline1.sync();
		
		//删除集合
		jedis.del(set);
		
		System.out.println("删除集合'" + set + "'完成");
	}
	
	/**
	 * 查询所有的active的节点的硬件信息，查找还有心跳信息的server
	 * 每个节点的信息作为list中的一个元素
	 */
	public List<String> getAllServerInfo() {
		List<String> ls = new ArrayList<String>();

		try {
			Set<String> keys = jedis.keys("mm.hb.*");

			for(String hp : keys) {
				String[] hostport = hp.substring(6).split(":");
				ls.add(getServerInfo(hostport[0], Integer.parseInt(hostport[1])));
			}
		} catch (Exception e) {
			System.out.println("Get mm.hb.* failed: " + e.getMessage());
		}

		return ls;
	}
	
	/**
	 * 查询某个节点的信息，指明节点名和端口号
	 */
	public String getServerInfo(String name, int port) {
		Socket s = new Socket();
		String temp = "";
		try {
			s.setSoTimeout(10000);				
			s.connect(new InetSocketAddress(name, port));
			temp += s.getRemoteSocketAddress() + "的信息:" + System.getProperty("line.separator");
			byte[] header = new byte[4];
			header[0] = ActionType.SERVERINFO;
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());
			DataInputStream dis = new DataInputStream(s.getInputStream());
			dos.write(header);
			dos.flush();
			int n = dis.readInt();
			temp += new String(readBytes(n, dis));
//			System.out.println(s.getRemoteSocketAddress()+"，信息获取成功");
			s.close();
		} catch(SocketTimeoutException e) {
			e.printStackTrace();
			temp = s.getRemoteSocketAddress()+e.getMessage();
		} catch(ConnectException e) {
			e.printStackTrace();
			temp = name+":"+e.getMessage();
		} catch (IOException e) {
			e.printStackTrace();
			temp = name+":" + e.getMessage();
		}
		return temp;
	}
	
	/**
	 * 从输入流中读取count个字节
	 * @param count
	 * @return
	 */
	public byte[] readBytes(int count, InputStream istream) {
		byte[] buf = new byte[count];			
		int n = 0;
		try {
			while(count > n) {
				n += istream.read(buf, n, count - n);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return buf;
	}
	
	public void closeJedis() {
		if(jedis != null)
			jedis.quit();
	}
}
