package iie.mm.tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import redis.clients.jedis.Jedis;

public class DedupInfo {

	public static void main(String[] args) {
		Jedis j1 = new Jedis("10.228.69.33",30999,60);
		Jedis j2 = new Jedis("10.228.69.37",30998,60);
//		byte[] b = jedis.dump("mm.dedup.info");
//		j2.restore("mm.dedup.info2", 0, b);
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_YEAR, -7);
		long ts = cal.getTimeInMillis()/1000;
		System.out.println(ts);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		
		for(String setmd5 : j1.hkeys("mm.dedup.info"))
		{
			String set = setmd5.split("@")[0];
			long time = 0;
			try{
				if(Character.isDigit(set.charAt(0)))
					time = Long.parseLong(set);
				else
					time = Long.parseLong(set.substring(1));
			}catch(NumberFormatException e){
				System.out.println("ignore timestamp "+set);
				continue;
			}
			String date = df.format(new Date(time*1000));
			j2.hset("mm.dedup."+date, setmd5, j1.hget("mm.dedup.info", setmd5));
			if(time <= ts)
			{
				j1.hdel("mm.dedup.info", setmd5);
				System.out.println("del hash field: "+setmd5);
			}
		}
	}

}
