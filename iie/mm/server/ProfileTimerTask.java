package iie.mm.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class ProfileTimerTask extends TimerTask {

	private double lastWn = 0;
	private long lastTs = System.currentTimeMillis();
	private String profileDir = "log/";
	private Jedis jedis;
	private String hbkey;
	
	public ProfileTimerTask(ServerConf conf, int period) {
		super();
		File dir = new File(profileDir);
		if(!dir.exists())
			dir.mkdirs();
		// 向redis的数据库1中插入心跳信息
		jedis = new RedisFactory(conf).getDefaultInstance();
		hbkey = "mm.hb." + conf.getNodeName() + ":" + conf.getServerPort();
		Pipeline pi = jedis.pipelined();
		pi.set(hbkey, "1");
		pi.expire(hbkey, period + 5);
		// 启动过的server
		pi.sadd("mm.active", conf.getNodeName() + ":" + conf.getServerPort());
		pi.sync();
	}

	@Override
	public void run() {
		double wn = ServerProfile.writtenBytes.longValue() / 1024.0;			//单位转换成KB
		long cur = System.currentTimeMillis();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String s = df.format(new Date());
		String bandwidth = s + " avg write bandwidth "+ (wn - lastWn) / ((cur - lastTs) / 1000.0) + " KB/s";
		String readDelay = "";
		if(ServerProfile.readN.longValue() == 0)
			readDelay = ", no read requests.";
		else
			readDelay = ", avg read latency " + (double)ServerProfile.readDelay.longValue() / ServerProfile.readN.longValue() + " ms";
		System.out.println(bandwidth + readDelay);
		
		lastWn = wn;
		lastTs = cur;
		
		ServerProfile.reset();
		
		//server的心跳信息
		jedis.expire(hbkey, 20);
		//把统计信息写入文件,每一天的信息放在一个文件里
		String profileName = s.substring(0, 10)+".txt";
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new FileOutputStream(profileDir+profileName,true));		//追加到文件尾
			pw.print(bandwidth);
			pw.println(readDelay);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(pw != null)
				pw.close();
		}
		
	}

}
