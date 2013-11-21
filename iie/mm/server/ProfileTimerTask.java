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
	public int period;
	private double lastWn = 0;
	private long lastDl = 0;
	private long lastDnr = 0;
	private long lastTs = System.currentTimeMillis();
	private String profileDir = "log/";
	private Jedis jedis;
	private String hbkey;
	
	public ProfileTimerTask(ServerConf conf, int period) {
		super();
		File dir = new File(profileDir);
		if (!dir.exists())
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
		this.period = period;
	}

	@Override
	public void run() {
		long cur = System.currentTimeMillis();
		double wn = ServerProfile.writtenBytes.longValue() / 1024.0;			//单位转换成KB
		double bw = (wn - lastWn) / ((cur - lastTs) / 1000.0);
		long dnr = ServerProfile.readN.longValue();
		long dl = ServerProfile.readDelay.longValue();
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String s = df.format(new Date());
		String info = s + " avg write bandwidth "+ bw + " KB/s";
		
		if ((dl - lastDl) == 0)
			info += ", no read requests.";
		else
			info += ", avg read latency " + (double)(dl - lastDl) / (dnr - lastDnr) + " ms";
		System.out.println(info);
		
		lastWn = wn;
		lastDnr = dnr;
		lastDl = dl;
		lastTs = cur;
		
		//server的心跳信息
		jedis.expire(hbkey, period + 5);
		
		//把统计信息写入文件,每一天的信息放在一个文件里
		String profileName = s.substring(0, 10)+".long";
		PrintWriter pw = null;
		try {
			//追加到文件尾
			pw = new PrintWriter(new FileOutputStream(profileDir + profileName, true));
			pw.println(info);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if(pw != null)
				pw.close();
		}
	}
}
