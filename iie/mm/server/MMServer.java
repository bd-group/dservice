package iie.mm.server;

import java.util.List;
import java.util.ArrayList;

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
		
		String serverName = null, redisServer = null;
		int serverPort = ServerConf.DEFAULT_SERVER_PORT, 
				redisPort = ServerConf.DEFAULT_REDIS_PORT, 
				blockSize = ServerConf.DEFAULT_BLOCK_SIZE, 
				period = ServerConf.DEFAULT_PERIOD,
				httpPort = ServerConf.DEFAULT_HTTP_PORT;
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h    : print this help.");
				System.out.println("-r    : local server name.");
				System.out.println("-p    : local server listen port.");
				System.out.println("-rr   : redis server name.");
				System.out.println("-rp   : redis server port.");
				System.out.println("-hp   : http server port.");
				System.out.println("-blk  : block size.");
				System.out.println("-prd  : logging period.");
				
				System.exit(0);
			}
			if (o.flag.equals("-r")) {
				// set serverName
				serverName = o.opt;
			}
			if (o.flag.equals("-p")) {
				// set serverPort
				serverPort = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-rr")) {
				// set redis server name
				redisServer = o.opt;
			}
			if (o.flag.equals("-rp")) {
				// set redis server port
				redisPort = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-hp")) {
				// set http server port
				httpPort = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-blk")) {
				// set block size
				blockSize = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-prd")) {
				// set logging period
				period = Integer.parseInt(o.opt);
			}
		}
		
		// set the serverConf
		try {
			conf = new ServerConf(serverName, serverPort, redisServer, redisPort, blockSize, period,httpPort);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
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
