package iie.mm.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import org.eclipse.jetty.server.Server;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class PhotoServer {
	public static long upts = System.currentTimeMillis();
	private ServerConf conf;
	private ServerSocket ss;
	private int serverport;
	private int period ;					//每隔period秒统计一次读写信息
	private ExecutorService pool;
	//集合跟到这个集合上的写操作队列的映射
	private ConcurrentHashMap<String, BlockingQueue<WriteTask>> sq = new ConcurrentHashMap<String, BlockingQueue<WriteTask>>();		
	
	public PhotoServer(ServerConf conf) throws Exception {
		this.conf = conf;
		serverport = conf.getServerPort();
		period = conf.getPeriod();
		if (!conf.isHTTPOnly()) {
			ss = new ServerSocket(serverport);
			pool = Executors.newCachedThreadPool();
		}
	}
	
	public void startUpHttp() throws Exception {
		Jedis jedis = new RedisFactory(conf).getDefaultInstance();
		if (jedis != null) {
			Set<Tuple> active = jedis.zrangeWithScores("mm.active.http", 0, -1);
			if (active != null && active.size() > 0) {
				for (Tuple t : active) {
					ServerConf.servers.put((long)t.getScore(), t.getElement());
					System.out.println("Got HTTP Server " + (long)t.getScore() + " " + t.getElement());
				}
			}
			jedis = RedisFactory.putInstance(jedis);
		}
		
		//启动http服务
		Server server = new Server(conf.getHttpPort());
		server.setHandler(new HTTPHandler(conf));
		server.start();
	}
	
	public void startUp() throws Exception {
		//服务端每隔一段时间进行一次读写速率统计,1秒后开始统计，每10秒输出一次平均信息
		Timer t = new Timer();
		t.schedule(new ProfileTimerTask(conf, period), 1 * 1000, period * 1000);
		
		//启动http服务
		Server server = new Server(conf.getHttpPort());
		server.setHandler(new HTTPHandler(conf));
		server.start();
		
		//启动监听写请求的服务,它使用junixsocket,所以需要用一个新的线程
		if (conf.isUse_junixsocket())
			new Thread(new WriteServer()).start();
		
		while(true) {
			try {
				// 接收tcp请求,来自tcp的请求是读取请求或者写请求
				pool.execute(new Handler(conf, ss.accept(), sq));
			} catch (IOException e) {
				e.printStackTrace();
				// BUG-XXX: do not shutdown the pool on any IOException.
				//pool.shutdown();
			}
		}
	}
	
	public static String getDNSHtml(ServerConf conf) {
		Jedis jedis = new RedisFactory(conf).getDefaultInstance();
		String r = "";
		
		if (jedis == null) 
			return "#FAIL: Get default jedis instance failed.";
		
		// get dns info
		Map<String, String> dns = jedis.hgetAll("mm.dns");

		if (dns != null && dns.size() > 0) {
			for (Map.Entry<String, String> e : dns.entrySet()) {
				r += e.getKey() + " -> " + e.getValue() + "<p/>";
			}
		} else {
			r += "Not available.<p>";
		}
		RedisFactory.putInstance(jedis);
		return r;
	}
	
	public static String getServerInfoHtml(ServerConf conf) {
		Jedis jedis = new RedisFactory(conf).getDefaultInstance();
		String r = "";
		
		if (jedis == null) 
			return "#FAIL: Get default jedis instance failed.";
		
		// find all http servers
		int configNr = 0, activeNr = 0;
		Set<Tuple> cservers = jedis.zrangeWithScores("mm.active.http", 0, -1);

		if (cservers != null) {
			r += "<H2> Total HTTP Servers: </H2><tt>";
			for (Tuple s : cservers) {
				String[] url = s.getElement().split(":");
				InetSocketAddress isa = new InetSocketAddress(url[0], 10000);
				r += "<p> <a href=http://" + isa.getAddress().getHostAddress() + ":" + url[1] + "/info><tt>[MMS" + (long)s.getScore() + "] " + s.getElement() + "</tt></a>";
			}
			configNr = cservers.size();
		}
		
		// find all mms servers
		Set<Tuple> mservers = jedis.zrangeWithScores("mm.active", 0, -1);
		HashMap<String, Long> smap = new HashMap<String, Long>();

		if (mservers != null) {
			for (Tuple s : mservers) {
				smap.put(s.getElement(), (long)s.getScore());
			}
		}
		
		// find heartbeated servers
		Set<String> aservers = jedis.keys("mm.hb.*");
		if (aservers != null) {
			r += "</tt><H2> Active MM Servers: </H2><tt>";
			for (String s : aservers) {
				r += "<p>[MMS" + smap.get(s.substring(6)) + "] " + s.substring(6);
			}
			r += "</tt>";
			activeNr = aservers.size();
		}
		if (activeNr < configNr) {
			r += "<H3><font color=\"red\">" + (configNr - activeNr) + " MM Server(s) might Down!</font></H3>";
		}
		RedisFactory.putInstance(jedis);
		
		return r;
	}
	
	public static String getSpaceInfoHtml(ServerConf conf) {
		String r = "";
		long free = 0;
		
		r += "<H2> Free Spaces (B): </H2><tt>";
		for (String dev : conf.getStoreArray()) {
			File f = new File(dev);
			r += "<p>" + dev + " -> " + "Total " + f.getTotalSpace() + ", Free " + f.getUsableSpace();
			free += f.getUsableSpace();
		}
		r += "<p> [Total Free] " + free + " (B)</tt>";
		return r;
	}
	
	public static String getServerInfo(ServerConf conf) {
		Jedis jedis = new RedisFactory(conf).getDefaultInstance();
		String r = "";
		
		if (jedis == null) 
			return "#FAIL: Get default jedis instance failed.";
		
		// find all servers
		Set<String> servers = jedis.zrange("mm.active", 0, -1);
		r += "\n Total  Servers:";
		for (String s : servers) {
			r += " " + s + ",";
		}
		// find heartbeated servers
		servers = jedis.keys("mm.hb.*");
		r += "\n Active Servers:";
		for (String s : servers) {
			r += " " + s.substring(6) + ",";
		}
		RedisFactory.putInstance(jedis);
		
		return r;
	}
	
	/**
	 * 专门用来接收写请求，使用junixsocket,应该可以实现并tcp更快的进程间通信
	 * @author zhaoyang
	 *
	 */
	class WriteServer implements Runnable {
		@Override
		public void run() {
			//在本机部署多个服务端,要修改
			final File socketFile = new File(new File(System.getProperty("java.io.tmpdir")), "junixsocket-test.sock");		
			ExecutorService pool = Executors.newCachedThreadPool(); 
			AFUNIXServerSocket server;
			 
			try {
				server = AFUNIXServerSocket.newInstance();
				server.bind(new AFUNIXSocketAddress(socketFile));
			    System.out.println("Start Unix Socket Server @ " + server.getInetAddress());
			    while (true) {
		            Socket sock = server.accept();
		            pool.execute((new Handler(conf, sock, sq)));
			    }
			} catch (IOException e) {
				e.printStackTrace();
				pool.shutdown();
			}
		}
	}
}

