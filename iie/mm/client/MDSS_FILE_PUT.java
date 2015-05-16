package iie.mm.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MDSS_FILE_PUT {

	public static class Option {
		String flag, opt;
		public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
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
	                    	optsList.add(new MDSS_FILE_PUT.Option(args[i], args[i+1]));
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
		int dupNum = 1;
		Set<String> sentinels = new HashSet<String>();
		String uri = null;
		String fname = null;
		String key = null;
		
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
				
				System.out.println("-stl  : sentinels <host:port;host:port>.");
				
				System.out.println("-uri  : unified uri for SENTINEL and STANDALONE.");
				
				System.out.println("(User used)");
				System.out.println("-k    : the target KEY");
				System.out.println("-f    : the file name to put");
				
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
			if (o.flag.equals("-f")) {
				// new file name
				fname = o.opt;
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
			if (o.flag.equals("-k")) {
				if (o.opt == null) {
					System.out.println("Please provide the KEY.");
					System.exit(0);
				}
				key = o.opt;
				System.out.println("get args: set=" + set + ", key=" + key);
			}
		}
		
		ClientAPI pcInfo = null;
		try {
			pcInfo = new ClientAPI();
			pcInfo.init(uri);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		if (fname == null) {
			System.out.println("Please provide valid file name by -f");
			System.exit(1);
		}
		if (key == null) {
			System.out.println("Please provide valid KEY by -k");
			System.exit(2);
		}
		
		byte[] content = null;
		File f = new File(fname);
		if (f.exists()) {
			try {
				FileInputStream in = new FileInputStream(f);
				content = new byte[(int) f.length()];
				in.read(content);
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(3);
			}
		}
		try {
			pcInfo.put(set + "@" + key, content);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

//		if (pcInfo.getPc().getConf().getRedisMode() == ClientConf.RedisMode.SENTINEL)
//			pcInfo.getPc().getRf().quit();
		pcInfo.quit();
	}

}
