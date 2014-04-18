package iie.mm.tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

/**
 * 多媒体元数据扫描工具
 * 检查副本分布情况，检查各个节点的数据情况，各个节点故障后可能丢失的数据量有多少
 */
public class MetadataChecker {

	private String rr;
	private int rp;
	
	private HashMap<String, String> sidname = new HashMap<String, String>();
	private HashMap<String, Integer> sidnum = new HashMap<String, Integer>();
	public MetadataChecker(String rr, int rp) {
		this.rr = rr;
		this.rp = rp;
		
		Jedis jedis = new Jedis(rr,rp);
		Set<Tuple> re = jedis.zrangeWithScores("mm.active", 0, -1);
		for(Tuple t : re)
		{
			String ipport = jedis.hget("mm.dns", t.getElement());
			if(ipport != null)
				sidname.put(((int)t.getScore())+"", ipport);
			else 
				sidname.put(((int)t.getScore())+"", t.getElement());
			sidnum.put(((int)t.getScore())+"", 0);
			System.out.println((int)t.getScore()+"  "+t.getElement());
		}
		jedis.quit();
	}

	public static class Option {
		String flag, opt;

		public Option(String flag, String opt) {
			this.flag = flag;
			this.opt = opt;
		}
	}

	private static List<Option> parseArgs(String[] args) {
		List<Option> optsList = new ArrayList<Option>();

		// parse the args
		for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
			case '-':
				if (args[i].length() < 2) {
					throw new IllegalArgumentException("Not a valid argument: "
							+ args[i]);
				}
				if (args[i].charAt(1) == '-') {
					if (args[i].length() < 3) {
						throw new IllegalArgumentException(
								"Not a valid argument: " + args[i]);
					}
				} else {
					if (args.length - 1 > i) {
						if (args[i + 1].charAt(0) == '-') {
							optsList.add(new Option(args[i], null));
						} else {
							optsList.add(new Option(args[i], args[i + 1]));
							i++;
						}
					} else {
						optsList.add(new Option(args[i], null));
					}
				}
				break;
			default:
				// arg
				break;
			}
		}

		return optsList;
	}

	public static void main(String[] args) {
		String rr = null;
		int rp = 0;
		long from = 0, to = System.currentTimeMillis()/1000;
		if (args.length >= 1) {
			List<Option> ops = parseArgs(args);

			for (Option o : ops) {
				if (o.flag.equals("-h")) {
					// print help message
					System.out.println("-h    : print this help.");
					System.out.println("-rr   : redis server ip.");
					System.out.println("-rp   : redis server port.");
					System.out.println("-tf   : timestamp where checker starts from, default is 0.");
					System.out.println("-tt   : timestamp where checker goes to, default is current time.");
					System.exit(0);
				}
				if (o.flag.equals("-rr")) {
					// set old metastore serverName
					if (o.opt == null) {
						System.out.println("-rr redis server ip. ");
						System.exit(0);
					}
					rr = o.opt;
				}
				if (o.flag.equals("-rp")) {
					if (o.opt == null) {
						System.out.println("-rp redis server port. ");
						System.exit(0);
					}
					rp = Integer.parseInt(o.opt);
				}
				if (o.flag.equals("-tf")) {
					if (o.opt == null) {
						System.out.println("-tf timestamp where checker starts from.");
						System.exit(0);
					}
					from = Long.parseLong(o.opt);
				}
				if (o.flag.equals("-tt")) {
					if (o.opt == null) {
						System.out.println("-tt timestamp where checker goes to. ");
						System.exit(0);
					}
					to = Long.parseLong(o.opt);
				}
			}
		}
		else{
			rr = "10.228.69.33";
			rp = 30999;
		}
		
		MetadataChecker checker = new MetadataChecker(rr, rp);
		checker.check(from, to);
	
	}
	//
	
	private HashMap<String, String> check(long from)
	{
		return this.check(from, System.currentTimeMillis()/1000);
	}
	
	private HashMap<String, String> check(long from, long to)
	{
		long start = System.currentTimeMillis();
		System.out.println("check redis " + rr + ":" + rp);
		int totalm = 0;
		HashMap<String, String> notdup = new HashMap<String, String>();
		Jedis jedis = new Jedis(rr, rp);

		Set<String> keys = new HashSet<String>();
		for (String s : jedis.keys("*.srvs")) {
			s = s.replaceAll("\\.srvs", "");
			try{
				if(s.charAt(0) == '1')
				{
					long ts = Long.parseLong(s);
					if(ts >= from && ts <= to)
						keys.add(s);
				}
				else
				{
					long ts = Long.parseLong(s.substring(1));
					if(ts >= from && ts <= to)
						keys.add(s);
				}
			}catch (NumberFormatException e) {
				System.out.println("Ignore timestamp: "+s);
				continue;
			}
		}
		for (String set : keys) {
			totalm += jedis.hlen(set);

			for (Map.Entry<String, String> en : jedis.hgetAll(set).entrySet()) {
				String[] infos = en.getValue().split("#");
				if (infos.length == 1) {
					notdup.put(set + "@" + en.getKey(), en.getValue());
					System.out.println("only one copy: " + set + "@"	+ en.getKey() + " --> " + en.getValue());
					String id = this.getServerid(infos[0]);
					int n = sidnum.get(id) + 1;
					sidnum.put(id, n);
				} else {
					String id = this.getServerid(infos[0]);
					boolean duped = false;
					for (int i = 1; i < infos.length; i++)
						if (!this.getServerid(infos[i]).equals(id))
							duped = true;
					if (!duped) {
						notdup.put(set + "@" + en.getKey(), en.getValue());
						System.out.println("all copies on same node: " + set + "@" + en.getKey() + " --> " + en.getValue());
						int n = sidnum.get(id) + 1;
						sidnum.put(id, n);
					}
				}
			}
		}

		System.out.println();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println("timestamp: "+from+" -- "+to+". date: "+ df.format(new Date(from*1000)) + " -- "+ df.format(new Date(to*1000)));
		System.out.println("check meta data takes " + (System.currentTimeMillis() - start) + " ms");
		System.out.println("checked objects num: " + totalm);
		for (Map.Entry<String, Integer> en : sidnum.entrySet()) {
			System.out.println("if server " + sidname.get(en.getKey()) + " down, " + en.getValue() + " mm objects may be lost.");
		}
		jedis.quit();
		
		return notdup;
	}
	
	/**
	 * type@set@serverid@block@offset@length@disk
	 * @param info
	 * @return
	 */
	public static String getServerid(String info)
	{
		String[] ss = info.split("@");
		if(ss.length != 7)
			throw new IllegalArgumentException("invalid info "+info);
		return ss[2];
	}
}
