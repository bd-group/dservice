package iie.metastore;

import java.util.Set;

import redis.clients.jedis.Jedis;

public class RedisChecker {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length == 0){
			System.out.println("please provide redis addr. <ip:port>");
			System.exit(0);
		}
//		args =new String[]{"localhost:6379"};	
		String[] ipport = args[0].split(":");
		int port = Integer.parseInt(ipport[1]);
		
		RedisChecker rc = new RedisChecker();
		rc.checkSFileStat(ipport[0], port);
		rc.checkSFileLocationStat(ipport[0], port);
	}
	
	private void checkSFileStat(String rr, int port)
	{
		System.out.println("Checking sfile stat...");
		Jedis jedis = new Jedis(rr,port);
		String pre = "sf.stat.";
		int n = 0;
		for(int s = 0;s <= 4; s++)
		{
			Set<String> ids = jedis.smembers(pre+s);
			if(ids == null || ids.size() == 0)
			{
				System.out.println(pre+s+" member size = 0");
				continue;
			}
			n += ids.size();
			System.out.println(pre+s+" member size = "+ids.size());
			for(String id : ids)
			{
				String file = jedis.hget("sfile", id);
				if(file == null)
				{
					System.out.println("ERROR: sf.stat. "+s+" contains file "+id+" which does not exist");
				}else{
					if(file.indexOf("\"store_status\":"+s) == -1)
					{
						System.out.println("ERROR: file "+id+" in sf.stat."+s+", but file is: "+file);
					}
				}
			}
			
		}
		
		System.out.println("hlen sfile = "+jedis.hlen("sfile"));
		System.out.println("all sf.stat. member size = "+n);
		System.out.println();
		jedis.quit();
	}

	private void checkSFileLocationStat(String rr, int port)
	{
		Jedis jedis = new Jedis(rr,port);
		System.out.println("Checking sfl stat...");
		String pre = "sfl.stat.";
		int n = 0;
		for(int s = 0;s <= 2; s++)
		{
			Set<String> ids = jedis.smembers(pre+s);
			if(ids == null || ids.size() == 0)
			{
				System.out.println(pre+s+" member size = 0");
				continue;
			}
			n += ids.size();
			System.out.println(pre+s+" member size = "+ids.size());
			for(String id : ids)
			{
				String location = jedis.hget("sfilelocation", id);
				if(location == null)
				{
					System.out.println("ERROR: sfl.stat. "+s+" contains location "+id+" which does not exist");
				}else{
					if(location.indexOf("\"visit_status\":"+s) == -1)
					{
						System.out.println("ERROR: filelocation "+id+" in sfl.stat."+s+", but location is: "+location);
					}
				}
			}
			
		}
		
		System.out.println("hlen sfilelocation = "+jedis.hlen("sfilelocation"));
		System.out.println("all sfl.stat. member size = "+n);
		System.out.println();
		jedis.quit();
	}
}
