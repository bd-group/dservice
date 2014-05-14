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
	public static FeatureIndex fi; 

	public ImageMatch(ServerConf conf) throws IOException {
		if (fi == null) {
			fi = new FeatureIndex(conf);
		}
		this.conf = conf;
	}

	public void startWork(int n) {
		for (int i = 0; i < n; i++)
			new Thread(new WorkThread(conf)).start();
	}
	
	public static void add(ImgKeyEntry en) {
		entries.add(en);
		sem.release();
	}
	
	public static ImgKeyEntry take() throws InterruptedException {
		sem.acquire();
		return entries.poll();
	}

	public static class ImgKeyEntry {
		private BufferedImage img;
		private String set;
		private String md5;
		private byte[] content;
		private int coff, clen;
		private String feature;
		
		public ImgKeyEntry(String feature, byte[] content, int coff, int clen, String set, String md5) {
			this.feature = feature;
			this.content = content;
			this.coff = coff;
			this.clen = clen;
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

	class WorkThread implements Runnable {
		private ServerConf conf;
		private ConcurrentLinkedQueue<String> failedq = new ConcurrentLinkedQueue<String>();
		
		public WorkThread(ServerConf conf) {
			this.conf = conf;
		}
		
		@Override
		public void run() {
			while (true) {
				try {
					ImgKeyEntry en = null;
					try {
						en = take();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					if (en == null)
						continue;
					
					if (en.getImg() == null) {
						BufferedImage bi;
						try {
							bi = ImageMatch.readImage(en.content, en.coff, en.clen);
						} catch (IOException e2) {
							e2.printStackTrace();
							continue;
						}
						if (bi == null)
							continue;
						else
							en.setImg(bi);
					}
					
					String hc = new ImagePHash().getHash(en.getImg());
					
					if (!fi.addObject(hc, en.feature, en.set + "@" + en.md5)) {
						failedq.add(en.feature + "|" + hc + "|" + en.set + "@" + en.md5);
						System.out.println("Feature " + en.feature + " " + hc + " -> " + en.set + "@" + en.md5);
					}
				} catch (Exception e) {
				}
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

	}
}
