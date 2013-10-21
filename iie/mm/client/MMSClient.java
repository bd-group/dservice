package iie.mm.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MMSClient {

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
		
		String localHostName = null, redisHost = null;
		String set = "default";
		int redisPort = 0;
		long lpt_nr = 1, lpt_size = 1;
		
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h    : print this help.");
				System.out.println("-r    : local host name.");
				System.out.println("-rr   : redis server name.");
				System.out.println("-rp   : redis server port.");
				
				System.out.println("-set  : specify set name.");
				System.out.println("-put  : put an object to server.");
				System.out.println("-get  : get an object from server by md5.");
				System.out.println("-getbi: get an object from server by INFO.");
				System.out.println("-del  : delete set from server.");
				
				System.out.println("-lpt  : large scacle put test.");
				
				System.exit(0);
			}
			if (o.flag.equals("-r")) {
				// set local host name
				localHostName = o.opt;
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
		}
		
		ClientConf conf = null;
		try {
			conf = new ClientConf(localHostName, redisHost, redisPort);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(0);
		}
		PhotoClient pc = new PhotoClient(conf);
		
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
					System.out.println("MD5:" + o.opt + " -> INFO: " + pc.storePhoto(set, o.opt, content));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-lpt")) {
				// large scale put test
				System.out.println("Provide the number of iterations, and mm object size.");
				System.out.println("LPT args: nr " + lpt_nr + ", size " + lpt_size);
				
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
						pc.storePhoto(set, sb.toString(), content);
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
			if (o.flag.equals("-get")) {
			}
			if (o.flag.equals("-getbi")) {
			}
			if (o.flag.equals("-del")) {
				
			}
		}
	}

}
