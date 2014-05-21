package iie.mm.server;

import iie.mm.client.Feature;
import iie.mm.client.ImagePHash;
import iie.mm.client.Feature.FeatureType;
import iie.mm.client.Feature.FeatureTypeString;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import javax.imageio.ImageIO;

import org.apache.lucene.document.Document;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.DocumentBuilderFactory;
import net.semanticmetadata.lire.imageanalysis.AutoColorCorrelogram;
import net.semanticmetadata.lire.imageanalysis.CEDD;
import net.semanticmetadata.lire.imageanalysis.ColorLayout;
import net.semanticmetadata.lire.imageanalysis.EdgeHistogram;
import net.semanticmetadata.lire.imageanalysis.FCTH;
import net.semanticmetadata.lire.imageanalysis.JCD;
import net.semanticmetadata.lire.imageanalysis.OpponentHistogram;
import net.semanticmetadata.lire.imageanalysis.PHOG;
import net.semanticmetadata.lire.imageanalysis.joint.JointHistogram;
import net.semanticmetadata.lire.impl.ChainedDocumentBuilder;
import net.semanticmetadata.lire.impl.GenericDocumentBuilder;
import net.semanticmetadata.lire.impl.SurfDocumentBuilder;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class FeatureSearch {
	private static Semaphore sem = new Semaphore(0);
	private static ConcurrentLinkedQueue<ImgKeyEntry> entries = new ConcurrentLinkedQueue<ImgKeyEntry>();

	private ServerConf conf;
	public static FeatureIndex fi;
	public static AtomicInteger isActive = new AtomicInteger(0);
	private static DocumentBuilder builder = FeatureSearch.getDocumentBuilder();

	public FeatureSearch(ServerConf conf) throws IOException {
		if (fi == null) {
			fi = new FeatureIndex(conf);
		}
		this.conf = conf;
	}
	
	public void startWork(int n) {
		for (int i = 0; i < n; i++) {
			Thread t = new Thread(new WorkThread(conf));
			t.setDaemon(true);
			t.start();
		}
	}
	
	public static int getQueueLength() {
		return entries.size();
	}
	
	public static boolean isEmpty() {
		return entries.isEmpty();
	}
	
	public static void add(ServerConf conf, ImgKeyEntry en) {
		if (conf.isIndexFeatures()) {
			entries.add(en);
			sem.release();
		}
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
		private List<FeatureType> features;
		
		public ImgKeyEntry(List<FeatureType> features, byte[] content, int coff, int clen, String set, String md5) {
			this.features = features;
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
						isActive.decrementAndGet();
						en = take();
						isActive.incrementAndGet();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					if (en == null)
						continue;
					
					if (en.getImg() == null) {
						BufferedImage bi;
						try {
							bi = FeatureSearch.readImage(en.content, en.coff, en.clen);
						} catch (IOException e2) {
							e2.printStackTrace();
							continue;
						}
						if (bi == null)
							continue;
						else
							en.setImg(bi);
					}
					
					for (FeatureType type : en.features) {
						switch (type) {
						case IMAGE_PHASH_ES: {
							String hc = new ImagePHash().getHash(en.getImg());

							if (!fi.addObject(hc, FeatureTypeString.IMAGE_PHASH_ES, en.set + "@" + en.md5)) {
								failedq.add(FeatureTypeString.IMAGE_PHASH_ES + "|" + hc + "|" + en.set + "@" + en.md5);
								System.out.println("Feature " + FeatureTypeString.IMAGE_PHASH_ES + " " + hc + " -> " + en.set + "@" + en.md5);
							}
							break;
						}
						case IMAGE_LIRE: {
							String key = en.set + "@" + en.md5;
							Document doc = builder.createDocument(en.getImg(), key);
							if (!fi.addObjectLIRE(doc, "LIRE", key)) {
								failedq.add(FeatureTypeString.IMAGE_LIRE + "|" + doc.getFields() + "|" + key);
								System.out.println("Feature " + FeatureTypeString.IMAGE_LIRE + " " + doc.getFields() + " -> " + key);
							}
							break;
						}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static DocumentBuilder getDocumentBuilder() {
        ChainedDocumentBuilder builder = new ChainedDocumentBuilder();
        /*builder.addBuilder(new GenericDocumentBuilder(CEDD.class, DocumentBuilder.FIELD_NAME_CEDD, true));
        builder.addBuilder(new GenericDocumentBuilder(PHOG.class, DocumentBuilder.FIELD_NAME_PHOG));
        builder.addBuilder(new GenericDocumentBuilder(FCTH.class, DocumentBuilder.FIELD_NAME_FCTH));
        builder.addBuilder(new GenericDocumentBuilder(JCD.class, DocumentBuilder.FIELD_NAME_JCD, true));
        builder.addBuilder(new GenericDocumentBuilder(OpponentHistogram.class, DocumentBuilder.FIELD_NAME_OPPONENT_HISTOGRAM));
        builder.addBuilder(new GenericDocumentBuilder(JointHistogram.class, DocumentBuilder.FIELD_NAME_JOINT_HISTOGRAM));
        builder.addBuilder(new GenericDocumentBuilder(ColorLayout.class, DocumentBuilder.FIELD_NAME_COLORLAYOUT));
        builder.addBuilder(new GenericDocumentBuilder(AutoColorCorrelogram.class, DocumentBuilder.FIELD_NAME_AUTOCOLORCORRELOGRAM));
        builder.addBuilder(new GenericDocumentBuilder(EdgeHistogram.class, DocumentBuilder.FIELD_NAME_EDGEHISTOGRAM));
        */
        builder.addBuilder(DocumentBuilderFactory.getHashingCEDDDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getAutoColorCorrelogramDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getFCTHDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getJCDDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getPHOGDocumentBuilder());        
        builder.addBuilder(DocumentBuilderFactory.getOpponentHistogramDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getJointHistogramDocumentBuilder());
        builder.addBuilder(DocumentBuilderFactory.getColorLayoutBuilder());
        builder.addBuilder(DocumentBuilderFactory.getEdgeHistogramBuilder());
        
        //builder.addBuilder(DocumentBuilderFactory.getCEDDDocumentBuilder());
        //builder.addBuilder(DocumentBuilderFactory.getScalableColorBuilder());
        //builder.addBuilder(DocumentBuilderFactory.getTamuraDocumentBuilder());
        //builder.addBuilder(DocumentBuilderFactory.getGaborDocumentBuilder());
        //builder.addBuilder(DocumentBuilderFactory.getColorHistogramDocumentBuilder());
        //builder.addBuilder(DocumentBuilderFactory.getJpegCoefficientHistogramDocumentBuilder());
        //builder.addBuilder(DocumentBuilderFactory.getLuminanceLayoutDocumentBuilder());
        //builder.addBuilder(new SurfDocumentBuilder());
        
        return builder;
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
