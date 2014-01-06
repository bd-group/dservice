package iie.metastore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class SysInfoStat {
	
	public static class DiskStatKey implements Comparable<DiskStatKey> {
		String hostname;
		String dev;
		
		public DiskStatKey(String hostname, String dev) {
			this.hostname = hostname;
			this.dev = dev;
		}
		
		@Override
		public int compareTo(DiskStatKey other) {
			System.out.println("THIS " + hostname + ", " + dev + "VS " + other.hostname + ", " + other.dev);
			return hostname.compareTo(other.hostname) & dev.compareTo(other.dev);
		}
	}
	
	public static class CPUStat {
		Double tlen = 0.0;
		int cpunr = 0;
		Long[] ts = new Long[2];
		Long[] user = new Long[2];
		Long[] nice = new Long[2];
		Long[] system = new Long[2];
		Long[] idle = new Long[2];
		Long[] iowait = new Long[2];
		
		public CPUStat() {
			cpunr = 0;
			ts[0] = new Long(0);
			ts[1] = new Long(0);
			user[0] = new Long(0);
			user[1] = new Long(0);
			nice[0] = new Long(0);
			nice[1] = new Long(0);
			system[0] = new Long(0);
			system[1] = new Long(0);
			idle[0] = new Long(0);
			idle[1] = new Long(0);
			iowait[0] = new Long(0);
			iowait[1] = new Long(0);
		}
		
		public String toString() {
			String r = "";
			
			r += tlen + "," +
					(ts[1] - ts[0]) + "," +
					(user[1] - user[0]) + "," +
					(nice[1] - nice[0]) + "," +
					(system[1] - system[0]) + "," +
					(idle[1] - idle[0]) + "," +
					(iowait[1] - iowait[0]) + "," + 
					cpunr;
			
			return r;
		}
		
		public void Update(Long cts, Long user, Long nice, Long system, Long idle, 
				Long iowait, int cpunr) {
			synchronized (this) {
				this.cpunr = cpunr;
				
				ts[0] = ts[1];
				this.user[0] = this.user[1];
				this.nice[0] = this.nice[1];
				this.system[0] = this.system[1];
				this.idle[0] = this.idle[1];
				this.iowait[0] = this.iowait[1];
				
				ts[1] = cts;
				this.user[1] = user;
				this.nice[1] = nice;
				this.system[1] = system;
				this.idle[1] = idle;
				this.iowait[1] = iowait;
			}
		}
	}
	
	public static class DiskStat {
		Double tlen = 0.0;
		Long[] ts = new Long[2];
		Long[] read = new Long[2];
		Long[] rmerge = new Long[2];
		Long[] rrate = new Long[2];
		Long[] rlat = new Long[2];
		Long[] write = new Long[2];
		Long[] wmerge = new Long[2];
		Long[] wrate = new Long[2];
		Long[] wlat = new Long[2];
		Long[] pIO = new Long[2];
		Long[] alat = new Long[2];
		Long[] wtlat = new Long[2];
		
		public DiskStat() {
			ts[0] = new Long(0);
			ts[1] = new Long(0);
			read[0] = new Long(0);
			read[1] = new Long(0);
			rmerge[0] = new Long(0);
			rmerge[1] = new Long(0);
			rrate[0] = new Long(0);
			rrate[1] = new Long(0);
			rlat[0] = new Long(0);
			rlat[1] = new Long(0);
			write[0] = new Long(0);
			write[1] = new Long(0);
			wmerge[0] = new Long(0);
			wmerge[1] = new Long(0);
			wrate[0] = new Long(0);
			wrate[1] = new Long(0);
			wlat[0] = new Long(0);
			wlat[1] = new Long(0);
			pIO[0] = new Long(0);
			pIO[1] = new Long(0);
			alat[0] = new Long(0);
			alat[1] = new Long(0);
			wtlat[0] = new Long(0);
			wtlat[1] = new Long(0);
		}
		
		public String toString() {
			String r = "";
		
			r += tlen + "," + 
					(ts[1] - ts[0]) + "," +
					(read[1] - read[0]) + "," +
					(rmerge[1] - rmerge[0]) + "," +
					(rrate[1] - rrate[0]) + "," +
					(rlat[1] - rlat[0]) + "," +
					(write[1] - write[0]) + "," +
					(wmerge[1] - wmerge[0]) + "," +
					(wrate[1] - wrate[0]) + "," +
					(wlat[1] - wlat[0]) + "," +
					(pIO[1]) + "," +
					(alat[1] - alat[0]) / (pIO[1] == 0 ? 1 : pIO[1]) + "," +
					(wtlat[1] - wtlat[0])/ (pIO[1] == 0 ? 1 : pIO[1]);
			return r;
		}
		
		public void Add(DiskStat other) {
			synchronized (this) {
				if (tlen == 0.0) {
					tlen = (double)(other.ts[1] - other.ts[0]);
					ts[0] = other.ts[0];
					ts[1] = other.ts[1];
				} else {
					tlen = (double) Math.min(ts[1] - ts[0], other.ts[1] - other.ts[0]);
					ts[0] = (ts[0] + other.ts[0]) / 2;
					ts[1] = (ts[1] + other.ts[1]) / 2;
				}
				read[0] += other.read[0];
				read[1] += other.read[1];
				rmerge[0] += other.rmerge[0];
				rmerge[1] += other.rmerge[1];
				rrate[0] += other.rrate[0];
				rrate[1] += other.rrate[1];
				rlat[0] += other.rlat[0];
				rlat[1] += other.rlat[1];
				write[0] += other.write[0];
				write[1] += other.write[1];
				wmerge[0] += other.wmerge[0];
				wmerge[1] += other.wmerge[1];
				wrate[0] += other.wrate[0];
				wrate[1] += other.wrate[1];
				wlat[0] += other.wlat[0];
				wlat[1] += other.wlat[1];
				pIO[0] += other.pIO[0];
				pIO[1] += other.pIO[1];
				alat[0] += other.alat[0];
				alat[1] += other.alat[1];
				wtlat[0] += other.wtlat[0];
				wtlat[1] += other.wtlat[1];
			}
		}
		
		public void Update(Long ts, Long read, Long rmerge, Long rrate, Long rlat, 
				Long write, Long wmerge, Long wrate, Long wlat,
				Long pIO, Long alat, Long wtlat) {
			synchronized (this) {
				this.ts[0] = this.ts[1];
				this.read[0] = this.read[1];
				this.rmerge[0] = this.rmerge[1];
				this.rrate[0] = this.rrate[1];
				this.rlat[0] = this.rlat[1];
				this.write[0] = this.write[1];
				this.wmerge[0] = this.wmerge[1];
				this.wrate[0] = this.wrate[1];
				this.wlat[0] = this.wlat[1];
				this.pIO[0] = this.pIO[1];
				this.alat[0] = this.alat[1];
				this.wtlat[0] = this.wtlat[1];

				this.ts[1] = ts;
				this.read[1] = read;
				this.rmerge[1] = rmerge;
				this.rrate[1] = rrate;
				this.rlat[1] = rlat;
				this.write[1] = write;
				this.wmerge[1] = wmerge;
				this.wrate[1] = wrate;
				this.wlat[1] = wlat;
				this.pIO[1] = pIO;
				this.alat[1] = alat;
				this.wtlat[1] = wtlat;
			}
		}
	}
	
	public static class Server implements Runnable {
		private boolean do_report = false;
		public int bsize = 65536;
		public int listenPort = 19888;
		public int interval = 5;
		public DatagramSocket server;
		public static Map<String, DiskStatKey> dskMap = new ConcurrentHashMap<String, DiskStatKey>();
		public static Map<DiskStatKey, DiskStat> dsMap = new ConcurrentHashMap<DiskStatKey, DiskStat>();
		public static Map<String, CPUStat> cpuMap = new ConcurrentHashMap<String, CPUStat>();
		public ServerReportTask spt = new ServerReportTask();
		public Timer timer = new Timer("ServerReportTask");
		public String prefix = "sysinfo"; 
		
		public Server(int port, int interval, String prefix) throws SocketException {
			if (port > 0)
				listenPort = port;
			this.interval = interval;
			server = new DatagramSocket(listenPort);
			timer.schedule(spt, 3000, 5000);
		}
		
		public class ServerReportTask extends TimerTask {
			public Map<String, DiskStat> nodeMap = new ConcurrentHashMap<String, DiskStat>();
			
			@Override
			public void run() {
				// handle SysInfoStat
				if (do_report) {
					Date d = new Date(System.currentTimeMillis());
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					File reportFile = new File(prefix + "-" + sdf.format(d));
					if (reportFile.getParentFile() != null)
						if (!reportFile.getParentFile().exists() && !reportFile.getParentFile().mkdirs()) {
							return;
						}
					
					StringBuffer sb = new StringBuffer(2048);
					
					for (Map.Entry<DiskStatKey, DiskStat> e : dsMap.entrySet()) {
						DiskStat ds = nodeMap.get(e.getKey().hostname);
						if (ds == null) {
							ds = new DiskStat();
							nodeMap.put(e.getKey().hostname, ds);
						}
						synchronized (e.getValue()) {
							ds.Add(e.getValue());
						}
						if (e.getValue().ts[1] - e.getValue().ts[0] < 1024)
							sb.append("RPT_DEV  -> " + e.getKey().hostname + "," + e.getKey().dev + "," + 
									(System.currentTimeMillis() / 1000) + "," + e.getValue() + "\n");
					}
					for (Map.Entry<String, DiskStat> e : nodeMap.entrySet()) {
						if (e.getValue().tlen > 0 && e.getValue().tlen < 1024) {
							sb.append("RPT_NODE -> " + e.getKey() + "," + 
									(System.currentTimeMillis() / 1000) + "," + e.getValue() + "\n");
						}
					}
					nodeMap.clear();
					
					for (Map.Entry<String, CPUStat> e : cpuMap.entrySet()) {
						if (e.getValue().ts[1] - e.getValue().ts[0] < 1024) 
							sb.append("RPT_CPU -> " + e.getKey() + "," +
									(System.currentTimeMillis() / 1000) + "," +
									e.getValue() + "\n");
					}
					
					// ok, write to file
					try {
						if (!reportFile.exists()) {
							reportFile.createNewFile();
						}
						FileWriter fw = new FileWriter(reportFile.getAbsoluteFile(), true);
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write(sb.toString());
						bw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					do_report = false;
				}
			}
		}

		@Override
		public void run() {
			while (true) {
				byte[] recvBuf = new byte[bsize];
				DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
				try {
					server.receive(recvPacket);
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}
				String recvStr = new String(recvPacket.getData(), 0, recvPacket.getLength());
				//System.out.println("RECV: " + recvStr);
				do_report = true;
				
				if (recvStr.length() <= 0)
					continue;
				
				String[] lines = recvStr.split("\n");
				
				for (String line : lines) {
					String[] ds = line.split(",");
					if (ds.length == 14) {
						// DiskStat
						DiskStatKey dsk = dskMap.get(ds[0] + ":" + ds[1]);
						if (dsk == null) {
							dsk = new DiskStatKey(ds[0], ds[1]);
							dskMap.put(ds[0] + ":" + ds[1], dsk);
						}
						DiskStat cds = dsMap.get(dsk);
						if (cds == null) {
							cds = new DiskStat();
							dsMap.put(dsk, cds);
							System.out.println("Alloc DISK " + dsk.hostname + ":" + dsk.dev);
						}
						if (Long.parseLong(ds[2]) > cds.ts[1]) {
							cds.Update(Long.parseLong(ds[2]), 
									Long.parseLong(ds[3]), 
									Long.parseLong(ds[4]), 
									Long.parseLong(ds[5]), 
									Long.parseLong(ds[6]), 
									Long.parseLong(ds[7]), 
									Long.parseLong(ds[8]), 
									Long.parseLong(ds[9]), 
									Long.parseLong(ds[10]), 
									Long.parseLong(ds[11]), 
									Long.parseLong(ds[12]), 
									Long.parseLong(ds[13]));
							//System.out.println("UPDATE -> " + cds);
						}
					} else if (ds.length == 11) {
						// CPUStat
						CPUStat cs = cpuMap.get(ds[0]);
						if (cs == null) {
							cs = new CPUStat();
							cpuMap.put(ds[0], cs);
							System.out.println("Alloc CPU " + ds[0]);
						}
						if (Long.parseLong(ds[1]) > cs.ts[1]) {
							cs.Update(Long.parseLong(ds[1]), 
									Long.parseLong(ds[3]),
									Long.parseLong(ds[4]),
									Long.parseLong(ds[5]),
									Long.parseLong(ds[6]),
									Long.parseLong(ds[7]),
									Integer.parseInt(ds[10]));
						}
					} else if (ds.length == 19) {
						// NetStat
					}
				}
			}
		}
	}
	
	public static class Client implements Runnable {
		public String serverName;
		public int port = 19888;
		public DatagramSocket client;
		public int interval = 5;
		
		public Client(String serverName, int port, int interval) throws SocketException {
			if (port > 0)
				this.port = port;
			this.serverName = serverName;
			this.interval = interval;
			client = new DatagramSocket();
		}

		@Override
		public void run() {
			while (true) {
				String sendStr = "";
				// read proc file system
				File diskstats = new File("/proc/diskstats");
				try {
					Thread.sleep(interval * 1000);
					FileReader fr = new FileReader(diskstats.getAbsoluteFile());
					BufferedReader br = new BufferedReader(fr);
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] s = line.split(" +");
						if (s.length == 15) {
							if (s[3].matches("sd[a-z]")) {
								sendStr += InetAddress.getLocalHost().getHostName() + "," +
										s[3] + "," +
										System.currentTimeMillis() / 1000 + "," +
										s[4] + "," + 
										s[5] + "," + s[6] + "," + s[7] + "," + 
										s[8] + "," + s[9] + "," + s[10] + "," + 
										s[11] + "," + s[12] + "," + s[13] + "," +
										s[14] + "\n";
							}
						}
					}
					br.close();
					fr.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				File sysstats = new File("/proc/stat");
				int cpunr = 0;
				try {
					FileReader fr = new FileReader(sysstats.getAbsoluteFile());
					BufferedReader br = new BufferedReader(fr);
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] s = line.split(" +");
						if (s[0].matches("cpu[0-9]+")) {
							cpunr++;
						}
					}
					br.close();
					fr.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					FileReader fr = new FileReader(sysstats.getAbsoluteFile());
					BufferedReader br = new BufferedReader(fr);
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] s = line.split(" +");
						if (s[0].equalsIgnoreCase("cpu")) {
							sendStr += InetAddress.getLocalHost().getHostName() + "," +
									System.currentTimeMillis() / 1000 + "," +
									s[0] + "," +
									s[1] + "," + 
									s[2] + "," + 
									s[3] + "," + 
									s[4] + "," + 
									s[5] + "," + 
									s[6] + "," + 
									s[7] + "," + 
									cpunr + "\n"; 
						}
					}
					br.close();
					fr.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.format("Got: {%s}\n", sendStr);
				
				if (sendStr.length() > 0) {
					byte[] sendBuf = sendStr.getBytes();
					DatagramPacket sendPacket;
					
					try {
						sendPacket = new DatagramPacket(sendBuf, sendBuf.length, 
								InetAddress.getByName(serverName), port);
						client.send(sendPacket);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}

	public static void main(String[] args) {
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    String serverName = "localhost";
	    String prefix = "sysinfo";
	    int mode = 0, port = -1, interval = 5;
	    
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
	                    	optsList.add(new SysInfoStat.Option(args[i], args[i+1]));
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
	    
	    for (Option o : optsList) {
	    	if (o.flag.equals("-h")) {
	    		// print help message
	    	}
	    	if (o.flag.equals("-s")) {
	    		// into server mode
	    		mode = 1;
	    	}
	    	if (o.flag.equals("-c")) {
	    		// into client mode
	    		mode = 0;
	    	}
	    	if (o.flag.equals("-r")) {
	    		// set serverName
	    		serverName = o.opt;
	    	}
	    	if (o.flag.equals("-p")) {
	    		// set serverPort
	    		port = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-i")) {
	    		// set interval
	    		interval = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-f")) {
	    		// set log file prefix
	    		prefix = o.opt;
	    	}
	    }
	    
	    try {
		    if (mode == 0) {
		    	// client mode
				Client cli = new Client(serverName, port, interval);
				cli.run();
		    } else {
		    	// server mode
		    	Server srv = new Server(port, 5, prefix);
		    	srv.run();
		    }
	    } catch (SocketException e) {
	    	e.printStackTrace();
	    }
	}
}
