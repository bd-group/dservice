package iie.mm.server;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

public class MMServer {
	public static ServerConf conf;
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}

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
	                    	optsList.add(new MMServer.Option(args[i], args[i+1]));
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
		
		String serverName = null,redisMasterName = null;
		int serverPort = ServerConf.DEFAULT_SERVER_PORT,
				blockSize = ServerConf.DEFAULT_BLOCK_SIZE, 
				period = ServerConf.DEFAULT_PERIOD,
				httpPort = ServerConf.DEFAULT_HTTP_PORT;
		Set<String> sa = new HashSet<String>();
		Set<String> stn = new HashSet<String>();
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h    : print this help.");
				System.out.println("-r    : local server name.");
				System.out.println("-p    : local server listen port.");
				System.out.println("-stn  : sentinels addr");
				System.out.println("-rmn  : redis master name");
				System.out.println("-hp   : http server port.");
				System.out.println("-blk  : block size.");
				System.out.println("-prd  : logging period.");
				System.out.println("-sa   : storage array.");
				
				System.exit(0);
			}
			if (o.flag.equals("-r")) {
				// set serverName
				if (o.opt == null) {
					System.out.println("-r serverName");
					System.exit(0);
				}
				serverName = o.opt;
			}
			if (o.flag.equals("-p")) {
				// set serverPort
				if (o.opt == null) {
					System.out.println("-p serverPort");
					System.exit(0);
				}
				serverPort = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-stn")) {
				// set redis server name
				if (o.opt == null) {
					System.out.println("-stn sentinels");
					System.exit(0);
				}
				for(String s : o.opt.split(";"))
					stn.add(s);
			}
			if (o.flag.equals("-rmn")) {
				if(o.opt == null){
					System.out.println("-rmn redis master name");
					System.exit(0);
				}
				redisMasterName = o.opt;
				
			}
			if (o.flag.equals("-hp")) {
				// set http server port
				httpPort = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-blk")) {
				// set block size
				if (o.opt == null) {
					System.out.println("-blk blockSize");
					System.exit(0);
				}
				blockSize = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-prd")) {
				// set logging period
				if (o.opt == null) {
					System.out.println("-prd period");
					System.exit(0);
				}
				period = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-sa")) {
				// parse storage array by ';'
				if (o.opt == null) {
					System.out.println("-sa path;path;path");
					System.exit(0);
				}
				String[] paths = o.opt.split(";");
				for (int i = 0; i < paths.length; i++) {
					sa.add(paths[i]);
				}
			}
		}
		
		// set the serverConf
		try {
			System.out.println(stn+redisMasterName);
			conf = new ServerConf(serverName, serverPort, blockSize, period, httpPort, redisMasterName, sa, stn);
//			conf.setStoreArray(sa);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		RedisFactory.init(conf);
		PhotoServer ps = null;
		try {
			ps = new PhotoServer(conf);
			ps.startUp();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

}
