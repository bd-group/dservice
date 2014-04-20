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
import java.util.TreeMap;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

/**
 * 多媒体元数据扫描工具
 * 检查副本分布情况，检查各个节点的数据情况，各个节点故障后可能丢失的数据量有多少
 */
public class MMRepChecker {
	private String rr;
	private int rp;
	private boolean checkNonStd = false;
	
	private HashMap<String, String> sidname = new HashMap<String, String>();
	private HashMap<String, Long> sidnum = new HashMap<String, Long>();
	
	public MMRepChecker(String rr, int rp, boolean checkNonStd) {
		this.rr = rr;
		this.rp = rp;
		
		Jedis jedis = new Jedis(rr, rp);
		Set<Tuple> re = jedis.zrangeWithScores("mm.active", 0, -1);
		
		if (re != null) {
			for (Tuple t : re) {
				String ipport = jedis.hget("mm.dns", t.getElement());
				if (ipport != null)
					sidname.put(((int)t.getScore()) + "", ipport);
				else 
					sidname.put(((int)t.getScore()) + "", t.getElement());
				sidnum.put(((int)t.getScore()) + "", 0l);
				System.out.println((int)t.getScore() + "  " + t.getElement());
			}
		}
		jedis.quit();
		this.checkNonStd = checkNonStd;
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
		long from = 0, to = System.currentTimeMillis() / 1000;
		boolean checkNonStd = false;
		
		if (args.length >= 1) {
			List<Option> ops = parseArgs(args);

			for (Option o : ops) {
				if (o.flag.equals("-h")) {
					// print help message
					System.out.println("-h    : print this help.");
					System.out.println("-rr   : redis server ip.");
					System.out.println("-rp   : redis server port.");
					System.out.println("-tf   : timestamp where checker starts from(included), default is 0.");
					System.out.println("-tt   : timestamp where checker goes to(excluded), default is current time.");
					System.exit(0);
				}
				if (o.flag.equals("-rr")) {
					// set redis server address
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
				if (o.flag.equals("-nsd")) {
					checkNonStd = true;
				}
			}
		} else{
			rr = "localhost";
			rp = 30999;
		}
		
		try {
			MMRepChecker checker = new MMRepChecker(rr, rp, checkNonStd);
			Map<String, String> undup = checker.check(from, to);
			System.out.println("Total unreplicated objects: " + undup.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public HashMap<String, String> check(long from) {
		return check(from, System.currentTimeMillis() / 1000);
	}
	
	public class SetStats {
		String set;
		long total_nr;
		long dup_nr;
		
		public SetStats(String set) {
			this.set = set;
			total_nr = 0;
			dup_nr = 0;
		}
		
		public String toString() {
			return "Set " + set + " -> Total " + total_nr + " nonRep " + dup_nr + " Health Ratio " + ((double)(total_nr - dup_nr) / total_nr * 100) + "%";
		}
	}
	
	public HashMap<String, String> check(long from, long to) {
		long start = System.currentTimeMillis();
		System.out.println("Check redis " + rr + ":" + rp);
		int totalm = 0;
		HashMap<String, String> notdup = new HashMap<String, String>();
		TreeMap<String, SetStats> setInfo = new TreeMap<String, SetStats>();
		Jedis jedis = new Jedis(rr, rp);

		Set<String> keys = new HashSet<String>();
		for (String s : jedis.keys("*.srvs")) {
			s = s.replaceAll("\\.srvs", "");
			try {
				if (Character.isDigit(s.charAt(0))) {
					long ts = Long.parseLong(s);
					if (ts >= from && ts < to)
						keys.add(s);
				} else {
					long ts = Long.parseLong(s.substring(1));
					if (ts >= from && ts < to)
						keys.add(s);
				}
			} catch (NumberFormatException e) {
				if (checkNonStd)
					keys.add(s);
				else
					System.out.println("Ignore timestamp: " + s);
				continue;
			}
		}
		System.out.println("A: only one copy");
		System.out.println("B: all copies on the same server\n");
		for (String set : keys) {
			long tnr = jedis.hlen(set);
			
			totalm += tnr;

			SetStats ss = setInfo.get(set);
			if (ss == null)
				ss = new SetStats(set);
			ss.total_nr = tnr;
			
			Map<String, String> setentrys = jedis.hgetAll(set);
			if (setentrys != null) {
				for (Map.Entry<String, String> en : setentrys.entrySet()) {
					String[] infos = en.getValue().split("#");
					if (infos.length == 1) {
						notdup.put(set + "@" + en.getKey(), en.getValue());
						System.out.println("A: " + set + "@"	+ en.getKey() + " --> " + en.getValue());
						String id = MMRepChecker.getServerid(infos[0]);
						long n = sidnum.get(id) + 1;
						sidnum.put(id, n);
						ss.dup_nr++;
					} else {
						String id = MMRepChecker.getServerid(infos[0]);
						boolean duped = true;
						for (int i = 1; i < infos.length; i++) {
							if (!MMRepChecker.getServerid(infos[i]).equals(id)) {
								duped = false;
								break;
							}
						}
						if (duped) {
							notdup.put(set + "@" + en.getKey(), en.getValue());
							System.out.println("B: " + set + "@" + en.getKey() + " --> " + en.getValue());
							long n = sidnum.get(id) + 1;
							sidnum.put(id, n);
							ss.dup_nr++;
						}
					}
				}
			}
			setInfo.put(set, ss);
		}

		System.out.println();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println("Timestamp: " + from + " -- " + to + ". Date: " + df.format(new Date(from * 1000)) + " -- " + df.format(new Date(to * 1000)));
		System.out.println("Check meta data takes " + (System.currentTimeMillis() - start) + " ms");
		System.out.println("Checked objects num: " + totalm + ", unreplicated: " + notdup.size() + ", healthy ratio " + ((double)(totalm - notdup.size()) / totalm * 100) + "%");
		for (Map.Entry<String, Long> en : sidnum.entrySet()) {
			System.out.println("If server " + sidname.get(en.getKey()) + " down, " + en.getValue() + " mm objects may be lost.");
		}
		System.out.println();
		System.out.println("Per-Set Replicate info: ");
		for (Map.Entry<String, SetStats> e : setInfo.descendingMap().entrySet()) {
			System.out.println(e.getValue());
		}
		jedis.quit();
		
		return notdup;
	}
	
	/**
	 * type@set@serverid@block@offset@length@disk
	 * @param info
	 * @return
	 */
	public static String getServerid(String info) {
		String[] ss = info.split("@");
		if (ss.length != 7)
			throw new IllegalArgumentException("Invalid info " + info);
		return ss[2];
	}
}
