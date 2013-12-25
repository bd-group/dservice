package iie.mm.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class MMSClient {

	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	public static class LPutThread extends Thread {
		private ClientAPI ca;
		public long pnr = 0;
		public long apnr = 0;
		public long size = 0;
		public String set = null;
		public String type = "";
		public long begin, end;
		
		public LPutThread(ClientAPI ca, String set, long pnr, long size, String type) {
			this.pnr = pnr;
			this.size = size;
			this.type = type;
			this.set = set;
			if(type.equalsIgnoreCase("pthca"))
			{
				this.ca = new ClientAPI();
				String uri = "STL://";
				for(String s :ca.getPc().getConf().getSentinels())
					uri = uri + s + ";";
				try {
					this.ca.init(uri.substring(0, uri.length()-1));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			else
				this.ca = ca;
		}
		
		public void run() {
			begin = System.nanoTime();
			try {
				if (type.startsWith("mput"))
				{
					MessageDigest md;
					md = MessageDigest.getInstance("md5");
					
					int pack = Integer.parseInt(type.split("_")[1]);
					Random r = new Random();
					for (int i = 0; i < pnr/pack; i++) {
						byte[][] content = new byte[pack][(int) size];
						String[] md5s = new String[pack];
						for(int l = 0;l<pack;l++)
						{
							r.nextBytes(content[l]);
							md.update(content[l]);
							byte[] mdbytes = md.digest();
							StringBuffer sb = new StringBuffer();
							for (int j = 0; j < mdbytes.length; j++) {
								sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
							}
							md5s[l] = sb.toString();
						}
						ca.mPut(set,md5s,content);
					}
				}
				else{
				for (int i = 0; i < pnr; i++) {
					byte[] content = new byte[(int) size];
					Random r = new Random();
					r.nextBytes(content);
					MessageDigest md;
					md = MessageDigest.getInstance("md5");
					md.update(content);
					byte[] mdbytes = md.digest();
			
					StringBuffer sb = new StringBuffer();
					for (int j = 0; j < mdbytes.length; j++) {
						sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
					}
					try {
						if (type.equalsIgnoreCase("sync"))
							ca.put(set + "@" + sb.toString(), content);
						else if (type.equalsIgnoreCase("async"))
							ca.put(set + "@" + sb.toString(), content);
						else if (type.equalsIgnoreCase("pthca"))
							ca.put(set + "@" + sb.toString(), content);
						else {
							if (type.equals(""))
								System.out.println("Please provide lpt_type");
							else
								System.out.println("Wrong lpt_type, should be sync, async, pthca or mput_{pack}");
							System.exit(0);
						}
						apnr++;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				}
				end = System.nanoTime();
				System.out.println(Thread.currentThread().getId() + " --> Put " + apnr + " objects in " +
						((end - begin) / 1000.0) + " us, PPS is " + (pnr * 1000000000.0) / (end - begin));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class LGetThread extends Thread {
		private ClientAPI ca;
		public Set<String> gets;
		public long gnr = 0;
		public long size = 0;
		public boolean doCheck;
		
		public long begin, end;
		
		public LGetThread(ClientAPI ca, Set<String> gets, boolean doCheck) {
			this.ca = ca;
			this.gets = gets;
			this.doCheck = doCheck;
		}
		
		public void run() {
			begin = System.nanoTime();
			for (String key : gets) {
				byte[] r = null;
				try {
					r = ca.get(key);

					if (doCheck) {
						MessageDigest md;
						md = MessageDigest.getInstance("md5");
						md.update(r);
						byte[] mdbytes = md.digest();
						StringBuffer sb = new StringBuffer();
						for (int j = 0; j < mdbytes.length; j++) {
							sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
						}
						String[] x = key.split("@");
						if (!sb.toString().equalsIgnoreCase(x[1])) {
							System.out.println("Key " + key + " is corrupt.");
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (r != null)
					size += r.length;
				gnr++;
			}
			end = System.nanoTime();
			System.out.println(Thread.currentThread().getId() + " --> Get " + gnr + " objects in " + 
					((end - begin) / 1000.0) + " us, GPS is " + (gnr * 1000000000.0) / (end - begin) + ", checked=" + doCheck);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    
	    // parse the args
		for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
	        case '-':
	            if (args[i].length() < 2)
	                throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	            if (args[i].charAt(1) == '-') {
	                if (args[i].length() < 3)
	                    throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	                doubleOptsList.add(args[i].substring(2, args[i].length()));
	            } else {
	                if (args.length-1 > i)
	                    if (args[i + 1].charAt(0) == '-') {
	                    	optsList.add(new Option(args[i], null));
	                    } else {
	                    	optsList.add(new MMSClient.Option(args[i], args[i+1]));
	                    	i++;
	                    }
	                else {
	                	optsList.add(new Option(args[i], null));
	                }
	            }
	            break;
	        default:
	            // arg
	            argsList.add(args[i]);
	            break;
	        }
		}
		
		String serverName = null, redisHost = null;
		String set = "default";
		int serverPort = 0, redisPort = 0;
		ClientConf.MODE mode = ClientConf.MODE.NODEDUP;
		long lpt_nr = 1, lpt_size = 1;
		int lgt_nr = -1, lgt_th = 1, lpt_th = 1;
		String lpt_type = "", lgt_type = "";
		int lmpt_nr = -1,lmpt_pack = -1,lmpt_size = -1;
		boolean lgt_docheck = false;
		int dupNum = 1;
		Set<String> sentinels = new HashSet<String>();
		String uri = null;
		
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h    : print this help.");
				System.out.println("-r    : server host name.");
				System.out.println("-p    : server port.");
				System.out.println("-rr   : redis server name.");
				System.out.println("-rp   : redis server port.");
				System.out.println("-m    : client operation mode.");
				System.out.println("-dn   : duplication number.");
				
				System.out.println("-set  : specify set name.");
				System.out.println("-put  : put an object to server.");
				System.out.println("-get  : get an object from server by md5.");
				System.out.println("-getbi: get an object from server by INFO.");
				System.out.println("-del  : delete set from server.");
				
				System.out.println("-lpt  : large scacle put test.");
				System.out.println("-lgt  : large scacle get test.");
				System.out.println("-lmpt : large scale mput test");
				System.out.println("-getserverinfo  :  get info from all servers online" ); 
				
				System.out.println("-stl  : sentinels <host:port;host:port>.");
				
				System.out.println("-uri  : unified uri for SENTINEL and STANDALONE.");
				
				System.exit(0);
			}
			if (o.flag.equals("-r")) {
				// set server host name
				serverName = o.opt;
			}
			if (o.flag.equals("-p")) {
				// set server port
				serverPort = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-m")) {
				// set client mode
				if (o.opt == null) {
					System.out.println("Please specify mode: [dedup, nodedup]");
				} else {
					if (o.opt.equalsIgnoreCase("dedup")) {
						mode = ClientConf.MODE.DEDUP;
					} else if (o.opt.equalsIgnoreCase("nodedup")) {
						mode = ClientConf.MODE.NODEDUP;
					}
				}
			}
			if(o.flag.equals("-dn")){
				//set duplication number
				if(o.opt == null){
					System.out.println("Please specify dn, or 1 is set by default");
				} else {
					dupNum = Integer.parseInt(o.opt);
					if(dupNum < 0) {
						System.out.println("dn must be positive.");
						System.exit(0);
					}
				}
			}
			if (o.flag.equals("-rr")) {
				// set redis server name
				redisHost = o.opt;
			}
			if (o.flag.equals("-rp")) {
				// set redis server port
				redisPort = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-set")) {
				// set the set name
				set = o.opt;
			}
			if (o.flag.equals("-lpt_nr")) {
				// set ltp nr
				lpt_nr = Long.parseLong(o.opt);
			}
			if (o.flag.equals("-lpt_size")) {
				// set lpt size
				lpt_size = Long.parseLong(o.opt);
			}
			if (o.flag.equals("-lpt_type")) {
				//sync or async
				lpt_type = o.opt;
			}
			if (o.flag.equals("-lgt_nr")) {
				lgt_nr = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-lgt_th")) {
				lgt_th = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-lpt_th")) {
				lpt_th = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-lgt_docheck")) {
				lgt_docheck = true;
			}
			if (o.flag.equals("-lgt_type")) {
				// get or search
				lgt_type = o.opt;
			}
			if (o.flag.equals("-lmpt_nr")){
				lmpt_nr = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-lmpt_pack")){
				lmpt_pack = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-lmpt_size")){
				lmpt_size = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-stl")) {
				// parse sentinels
				if (o.opt == null) {
					System.out.println("-stl host:port;host:port");
					System.exit(0);
				}
				String[] stls = o.opt.split(";");
				for (int i = 0; i < stls.length; i++) {
					sentinels.add(stls[i]);
				}
			}
			if (o.flag.equals("-uri")) {
				// parse uri
				if (o.opt == null) {
					System.out.println("-uri URI");
					System.exit(0);
				}
				uri = o.opt;
			}
		}
		
		/*ClientConf conf = null;
		try {
			if (sentinels.size() > 0)
				conf = new ClientConf(sentinels, mode, dupNum);
			else
				conf = new ClientConf(serverName, serverPort, redisHost, redisPort, mode, dupNum);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(0);
		}*/
		ClientAPI pcInfo = null;
		try {
			//pcInfo = new ClientAPI(conf);
			//pcInfo.init(redisHost + ":" + redisPort);
			pcInfo = new ClientAPI();
			pcInfo.init(uri);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		for (Option o : optsList) {
			if (o.flag.equals("-put")) {
				if (o.opt == null) {
					System.out.println("Please provide the put target.");
					System.exit(0);
				}
				byte[] content = null;
				File f = new File(o.opt);
				if (f.exists()) {
					try {
						FileInputStream in = new FileInputStream(f);
						content = new byte[(int) f.length()];
						in.read(content);
						in.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					MessageDigest md;
					md = MessageDigest.getInstance("md5");
					md.update(content);
					byte[] mdbytes = md.digest();
			
					StringBuffer sb = new StringBuffer();
					for (int j = 0; j < mdbytes.length; j++) {
						sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
					}
					System.out.println("MD5: " + sb.toString() + " -> INFO: " + pcInfo.put(set + "@" + sb.toString(), content));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-lpta")) {
				// large scale put test with atomic interface
				System.out.println("Provide the number of iterations, and mm object size.");
				System.out.println("LPT args: nr " + lpt_nr + ", size " + lpt_size +", type " + lpt_type);
				
				List<LPutThread> lputs = new ArrayList<LPutThread>();
				for (int i = 0; i < lpt_th; i++) {
					lputs.add(new LPutThread(pcInfo, set, lpt_nr / lpt_th, lpt_size, lpt_type));
				}
				long begin = System.currentTimeMillis();
				for (LPutThread t : lputs) {
					t.start();
				}
				lpt_nr = 0;
				for (LPutThread t : lputs) {
					t.join();
					lpt_nr += t.pnr;
				}
				long end = System.currentTimeMillis();
				System.out.println("LPTA nr " + lpt_nr + " size " + lpt_size +
						": BW " + lpt_size * lpt_nr * 1000.0 / 1024.0 / (end - begin) + " KBps," + 
						" LAT " + (end - begin) / (double)lpt_nr + " ms");
			}
			if (o.flag.equals("-lpt")) {
				// large scale put test
				System.out.println("Provide the number of iterations, and mm object size.");
				System.out.println("LPT args: nr " + lpt_nr + ", size " + lpt_size +", type " + lpt_type);
				
				try {
					long begin = System.currentTimeMillis();
					
					for (int i = 0; i < lpt_nr; i++) {
						byte[] content = new byte[(int) lpt_size];
						Random r = new Random();
						r.nextBytes(content);
						MessageDigest md;
						md = MessageDigest.getInstance("md5");
						md.update(content);
						byte[] mdbytes = md.digest();
				
						StringBuffer sb = new StringBuffer();
						for (int j = 0; j < mdbytes.length; j++) {
							sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
						}
						if (lpt_type.equalsIgnoreCase("sync"))
							pcInfo.put(set + "@" + sb.toString(), content);
						else if (lpt_type.equalsIgnoreCase("async"))
							pcInfo.put(set + "@" + sb.toString(), content);
						else {
							if (lpt_type.equals(""))
								System.out.println("Please provide lpt_type");
							else
								System.out.println("Wrong lpt_type, should be sync or async");
							System.exit(0);
						}
					}
					long end = System.currentTimeMillis();
					System.out.println("LPT nr " + lpt_nr + " size " + lpt_size + 
							": BW " + lpt_size * lpt_nr * 1000.0 / 1024.0 / (end - begin) + " KBps," + 
							" LAT " + (end - begin) / (double)lpt_nr + " ms");
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if (o.flag.equals("-lgta")) {
				// large scale get test with atomic interface
				System.out.println("Provide the number of iterations.");
				System.out.println("LGT args: nr " + lgt_nr);
				if (lgt_nr == -1) {
					System.out.println("please provide number of iterations using -lgt_nr");
					System.exit(0);
				}
				try {
					Map<String, String> stored = pcInfo.getPc().getNrFromSet(set);
					if (stored.size() < lgt_nr) {
						lgt_nr = stored.size();
					}
					int i = 0;
					long size = 0;
					ArrayList<Set<String>> gets = new ArrayList<Set<String>>();
					List<LGetThread> lgets = new ArrayList<LGetThread>();
					for (i = 0; i < lgt_th; i++) {
						gets.add(new HashSet<String>());
						lgets.add(new LGetThread(pcInfo, gets.get(i), lgt_docheck));
					}
					i = 0;
					if (lgt_type.equalsIgnoreCase("get")) {
						for (String key : stored.keySet()) {
							gets.get(i % lgt_th).add(set + "@" + key);
							i++;
							if (i >= lgt_nr)
								break;
						}
					} else if (lgt_type.equalsIgnoreCase("search")) {
						for (String val : stored.values()) {
							gets.get(i % lgt_th).add(val);
							i++;
							if (i >= lgt_nr)
								break;
						}
					} else {
						if (lgt_type.equals("")) 
							System.out.println("Please provide lgt_type.");
						else 
							System.out.println("Wrong lgt_type, expect 'get' or 'search'");
						System.exit(0);
					}
					long begin = System.currentTimeMillis();
					for (LGetThread t : lgets) {
						t.start();
					}
					lgt_nr = 0;
					size = 0;
					for (LGetThread t : lgets) {
						t.join();
						lgt_nr += t.gnr;
						size += t.size;
					}
					long dur = System.currentTimeMillis() - begin;
					System.out.println("LGTA nr " + lgt_nr + " size " + size + "B " + 
							": BW " + (size * 1000 / 1024.0 / (dur)) + " KBps," + 
							" AVG LAT " + ((double)dur / lgt_nr) + " ms");
				} catch(IOException e){
					e.printStackTrace();
				}
			}
			
			if (o.flag.equals("-lgt")) {
				// large scale get test
				System.out.println("Provide the number of iterations.");
				System.out.println("LGT args: nr " + lgt_nr);
				if (lgt_nr == -1) {
					System.out.println("please provide number of iterations using -lgt_nr");
					System.exit(0);
				}
				try {
					Map<String, String> stored = pcInfo.getPc().getNrFromSet(set);
					if (stored.size() < lgt_nr) {
						lgt_nr = stored.size();
					}
					int i = 0;
					long size = 0;
					long begin = System.currentTimeMillis();
					
					if (lgt_type.equalsIgnoreCase("get")) {
						for (String key : stored.keySet()) {
							byte[] r = pcInfo.get(set + "@" + key);
							if (r != null)
								size += r.length;
							i++;
							if (i >= lgt_nr)
								break;
						}
					} else if (lgt_type.equalsIgnoreCase("search")) {
						for (String val : stored.values()) {
							byte[] r = pcInfo.get(val);
							if (r != null)
								size += r.length;
							i++;
							if (i >= lgt_nr)
								break;
						}
					} else {
						if (lgt_type.equals("")) 
							System.out.println("Please provide lgt_type.");
						else 
							System.out.println("Wrong lgt_type, expect 'get' or 'search'");
						System.exit(0);
					}
					long dur = System.currentTimeMillis() - begin;
					System.out.println("LGT nr " + lgt_nr + " size " + size + "B " + 
							": BW " + (size * 1000 / 1024.0 / (dur)) + " KBps," + 
							" LAT " + ((double)dur / lgt_nr) + " ms");
				} catch(IOException e){
					e.printStackTrace();
				}
			}
			
			if(o.flag.equals("-lmpt")){
				if(lmpt_nr < 0 || lmpt_pack < 0 || lmpt_size < 0)
				{
					System.out.println("please provide lmpt_nr,lmpt_pack,lmpt_size");
					System.exit(0);
				}
				System.out.println("LMPT args: nr " + lmpt_nr + ", size " + lmpt_size +", package number "+lmpt_pack );
				
				try {
					long begin = System.currentTimeMillis();
					Random r = new Random();
					MessageDigest md;
					md = MessageDigest.getInstance("md5");
					
					for (int i = 0; i < lmpt_nr/lmpt_pack; i++) {
						byte[][] content = new byte[lmpt_pack][lmpt_size];
						String[] md5s = new String[lmpt_pack];
						for(int l = 0;l<lmpt_pack;l++)
						{
							r.nextBytes(content[l]);
							md.update(content[l]);
							byte[] mdbytes = md.digest();
							StringBuffer sb = new StringBuffer();
							for (int j = 0; j < mdbytes.length; j++) {
								sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
							}
							md5s[l] = sb.toString();
						}
						pcInfo.mPut(set,md5s,content);
					}
					long end = System.currentTimeMillis();
					System.out.println("LPT nr " + lmpt_nr + " size " + lmpt_size + 
							": BW " + lmpt_size * lmpt_nr * 1000.0 / 1024.0 / (end - begin) + " KBps," + 
							" LAT " + (end - begin) / (double)lmpt_nr + " ms");
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-get")) {
				if (o.opt == null) {
					System.out.println("Please provide the get md5.");
					System.exit(0);
				}
				String md5 = o.opt;
				System.out.println("Provide the set and md5 about a photo.");
				System.out.println("get args: set " + set + ", md5 " + md5);
				
				try {
					byte[] content = pcInfo.get(set + "@" + md5);
					FileOutputStream fos = new FileOutputStream(md5);
					fos.write(content);
					fos.close();
					System.out.println("Get content length: " + content.length);
				} catch(IOException e){
					e.printStackTrace();
				}
			}
			
			if (o.flag.equals("-getbi")) {
				String info = o.opt;
				System.out.println("Provide the INFO about a photo.");
				System.out.println("get args:  " + info);
				try{
					
					byte[] content = pcInfo.get(info);
					FileOutputStream fos = new FileOutputStream("getbi");
					fos.write(content);
					fos.close();
					System.out.println("get content length:"+content.length);
				}catch(IOException e){
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-del")) {
				String sname = o.opt;
				System.out.println("Provide the set name to be deleted.");
				System.out.println("get args: set name  " + sname);
				DeleteSet ds = new DeleteSet(redisHost, redisPort);
				ds.delSet(sname);
				ds.closeJedis();
			}
			if (o.flag.equals("-getserverinfo")) {
				System.out.println("get server info.");
				DeleteSet ds = new DeleteSet(redisHost, redisPort);
				List<String> ls = ds.getAllServerInfo();
				if(ls == null) {
					System.out.println("出现错误");
					return;
				}
				for(String s : ls) {
					System.out.println(s);
				}
			}
		}
		if (pcInfo.getPc().getConf().getRedisMode() == ClientConf.RedisMode.SENTINEL)
			pcInfo.getPc().getRf().quit();
		pcInfo.quit();
	}

}
