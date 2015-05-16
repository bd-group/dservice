package iie.mm.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MDSS_FILE {

	public static class Option {
		String flag, opt, opt2 = null;
		public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
		public Option(String flag, String opt1, String opt2) {
			this.flag = flag;
			opt = opt1;
			this.opt2 = opt2;
		}
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
	            	boolean hit = false;
	            	
	            	if (args.length - 2 > i) {
	            		if (args[i + 1].charAt(0) != '-' && 
	            				args[i + 2].charAt(0) != '-') {
	            			System.out.println(args[i]+","+args[i+1]+","+args[i+2]);
	            			optsList.add(new Option(args[i], args[i+1], args[i+2]));
	            			i += 2;
	            			hit = true;
	            		}
	            	}
	            	if (!hit) {
	            		if (args.length-1 > i)
	            			if (args[i + 1].charAt(0) == '-') {
	            				optsList.add(new Option(args[i], null));
	            			} else {
	            				optsList.add(new Option(args[i], args[i+1]));
	            				i++;
	            			}
	            		else {
	            			optsList.add(new Option(args[i], null));
	            		}
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
				System.out.println("-put  : put an object to server.");
				System.out.println("-get  : get an object from server by key.");
				System.out.println("-ls   : list the directory.");
				System.out.println("-mk   : make a new directory.");

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
		
		for (Option o : optsList) {
			if (o.flag.equals("-put")) {
				String dst_file = o.opt2;
				
				if (o.opt == null) {
					System.out.println("-put local_file dst_file");
					System.out.println();
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
				if (dst_file == null)
					dst_file = "/";
				if (dst_file.endsWith("/")) {
					// dst is a directory: mkdir and generate new name
					if (dst_file.length() > 1) {
						try {
							if (pcInfo.mkdir(set, dst_file) == 2) {
								System.out.println("Failed to create DIR: " + dst_file);
								System.exit(1);
							}
						} catch (Exception e) {
							e.printStackTrace();
							System.exit(1);
						}
					}
					dst_file += f.getName();
				} else {
					dst_file = o.opt2;
					try {
						String parent = dst_file.substring(0, dst_file.lastIndexOf("/"));
						if (parent.length() > 1) {
							if (pcInfo.mkdir(set, parent) == 2) {
								System.out.println("Failed to create DIR: " + parent);
								System.exit(1);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
				}
				try {
					System.out.println("INFO: " + pcInfo.put(set + "@" + dst_file, content));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-get")) {
				String dst_file = o.opt2;
				
				if (o.opt == null) {
					System.out.println("Please provide the get KEY.");
					System.exit(0);
				}
				String key = o.opt;
				System.out.println("get args: set=" + set + ", key=" + key);
				
				if (dst_file != null) {
					// assume it is a valid file location
					if (dst_file.endsWith("/")) {
						String fname = key.substring(key.lastIndexOf("/"), key.length());
						if (fname.length() == 0) {
							System.out.println("Want to get a DIRECTORY?");
							System.exit(1);
						}
						dst_file = dst_file + fname;
					}
				} else {
					if (dst_file == null) {
						dst_file = key.substring(key.lastIndexOf("/"), key.length());
						if (dst_file.length() == 0) {
							System.out.println("Want to get a DIRECTORY?");
							System.exit(1);
						}
						dst_file = key;
					}
					if (dst_file.endsWith("/")) {
						System.out.println("Invalid local_file_name.");
						System.exit(1);
					} else {
						int start = dst_file.lastIndexOf("/");
						System.out.println("XXDST:" + dst_file + "," + start);
						if (start > 0) {
							dst_file = dst_file.substring(start + 1, dst_file.length());
							System.out.println("DST:" + dst_file + "," + start);
						}
					}
				}
				try {
					byte[] content = pcInfo.get(set + "@" + key);
					FileOutputStream fos = new FileOutputStream(dst_file);
					fos.write(content);
					fos.close();
					System.out.println("Get content length: " + content.length);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-ls")) {
				try {
					System.out.println(pcInfo.list(set, o.opt));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-mk")) {
				try {
					System.out.println(pcInfo.mkdir(set, o.opt));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		pcInfo.quit();
	}

}
