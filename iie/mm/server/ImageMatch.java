package iie.mm.server;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import javax.imageio.ImageIO;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class ImageMatch {
	private static Semaphore sem = new Semaphore(0);
	private static ConcurrentLinkedQueue<ImgKeyEntry> entries = new ConcurrentLinkedQueue<ImgKeyEntry>();
	
	private ServerConf conf;
	
	public ImageMatch(ServerConf conf)
	{
		this.conf = conf;
	}
	
	public void startWork(int n)
	{
		for(int i = 0;i<n;i++)
			new Thread(new WorkThread(conf)).start();
	}
	public static void add(ImgKeyEntry en)
	{
		entries.add(en);
		sem.release();
	}
	public static ImgKeyEntry take() throws InterruptedException
	{
		sem.acquire();
		return entries.poll();
	}

	public static class ImgKeyEntry
	{
		private BufferedImage img;
		private String set;
		private String md5;
		public ImgKeyEntry(BufferedImage img, String set, String md5) {
			this.img = img;
			this.set = set;
			this.md5 = md5;
		}
		public BufferedImage getImg() {
			return img;
		}
		public void setImg(BufferedImage img) {
			this.img = img;
		}
		public String getSet() {
			return set;
		}
		public void setSet(String set) {
			this.set = set;
		}
		public String getMd5() {
			return md5;
		}
		public void setMd5(String md5) {
			this.md5 = md5;
		}
		
	}
	
	private class WorkThread implements Runnable
	{
		private RedisFactory rf;
		private ServerConf conf;
		private ConcurrentLinkedQueue<String> failedq = new ConcurrentLinkedQueue<String>();
		public WorkThread(ServerConf conf)
		{
			this.conf = conf;
			this.rf = new RedisFactory(this.conf);
		}
		@Override
		public void run() {
			while(true)
			{
				ImgKeyEntry en = null;
				try {
					en = take();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				if(en == null)
					continue;
				String hc = new ImagePHash().getHash(en.getImg());
				String set = en.getSet();
				String md5 = en.getMd5();
				while(true)		//循环退出的条件:写入redis成功且失败队列为空，或者写入redis失败
				{
					try{
						sendRedis(hc,set,md5);
						if(!failedq.isEmpty()){
							String h = failedq.poll();
							System.out.println("in ImageMatch, retry put "+h);
							if(h != null)
							{
								String[] hsm = h.split("@");
								hc = hsm[0];
								set = hsm[1];
								md5 = hsm[2];
							}
						}
						else
							break;
					}
					catch(JedisConnectionException e) {
						e.printStackTrace();
						failedq.add(hc+"@"+set+"@"+md5);
						System.out.println("#FAIL: in ImageMatch,put "+hc+"@"+set+"@"+md5+" into redis failed.");
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						break;
					}
				}
			}
		}
		
		private void sendRedis(String phash, String set, String md5) throws JedisConnectionException
		{
			Jedis jedis = rf.getDefaultInstance();
			try{
				if(jedis == null)
					throw new JedisConnectionException("failed to connect to redis.");
				jedis.hset(set+".imagematch", phash, set+"@"+md5);
				
			}catch(JedisConnectionException e){
				RedisFactory.putBrokenInstance(jedis);
				jedis = null;
				throw new JedisConnectionException(e);
			}
			finally{
				if(jedis != null)
					RedisFactory.putInstance(jedis);
			}
		}
		
	}
	
	
	public static BufferedImage readImage(byte[] b, int offset, int length) throws IOException
	{
		ByteArrayInputStream in = new ByteArrayInputStream(b, offset, length);    
		BufferedImage image = ImageIO.read(in);     
		return image;
	}
	public static BufferedImage readImage(byte[] b) throws IOException
	{
		ByteArrayInputStream in = new ByteArrayInputStream(b);    
		BufferedImage image = ImageIO.read(in);    
		return image;
	}
	
	/**
	 * 计算汉明距离
	 * @param s1 指纹数1
	 * @param s2 指纹数2
	 * @return 汉明距离
	 */
	public static int distance(String s1, String s2) {
		int count = 0;
//		System.out.println("in distance, s1:"+s1);
//		System.out.println("in distance, s2:"+s2);
		for(int i=0; i<s1.length(); i++) {
			if(s1.charAt(i) != s2.charAt(i)) {
				count ++;
			}
		}
		return count;
	}
	
	/**
	 * 把二进制的字符串转换成16进制形式，bi长度得是4的倍数
	 * @param bi
	 * @return
	 */
	public static String binToHex(String bi)
	{
		int len = bi.length();
		String hex = "";
		for(int i = len -1; i > 0;i -= 4)
		{
			String sub = bi.substring(i-3 >= 0? i-3:0, i+1);
			String a = Integer.toHexString(Integer.parseInt(sub,2));
			hex = a + hex;
		}
		return hex;
	}
	
	/**
	 * 十六进制字符串转换成二进制，每个十六进制字符转换成4位二进制
	 * @param hex
	 * @return
	 */
	public static String hexToBin(String hex)
	{
		String bin = "";
		for(int i = 0;i<hex.length();i++)
		{
			String s = Integer.toBinaryString(Integer.parseInt(hex.charAt(i)+"",16));
			while(s.length() < 4)
				s = "0"+s;
			bin += s;
		}
		return bin;
	}
	
	public static void main(String[] a) 
	{
//		ImageMatch.WorkThread p = new ImageMatch(conf)ImageMatch.new
	}
}
