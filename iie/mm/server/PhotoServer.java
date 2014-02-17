package iie.mm.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
		ss = new ServerSocket(serverport);
		pool = Executors.newCachedThreadPool();
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
	
	public static String getServerInfoHtml(ServerConf conf) {
		Jedis jedis = new RedisFactory(conf).getDefaultInstance();
		String r = "";
		
		if (jedis == null) 
			return "#FAIL: Get default jedis instance failed.";
		
		// find all servers
		Set<String> servers = jedis.zrange("mm.active.http", 0, -1);
		r += "<H2> Total HTTP Servers: </H2><tt>";
		for (String s : servers) {
			String[] url = s.split(":");
			InetSocketAddress isa = new InetSocketAddress(url[0], 10000);
			r += "<p> <a href=http://" + isa.getAddress().getHostAddress() + ":" + url[1] + "/info><tt>" + s + "</tt></a>";
		}
		// find heartbeated servers
		servers = jedis.keys("mm.hb.*");
		r += "</tt><H2> Active MM Servers: </H2><tt>";
		for (String s : servers) {
			r += "<p>" + s.substring(6);
		}
		r += "</tt>";
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

