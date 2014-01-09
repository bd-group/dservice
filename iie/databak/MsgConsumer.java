package iie.databak;

import iie.databak.DatabakConf.RedisInstance;
import iie.databak.DatabakConf.RedisMode;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.thrift.TException;

import redis.clients.jedis.exceptions.JedisConnectionException;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class MsgConsumer {

	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	private static List<Option> parseArgs(String[] args)
	{
	    List<Option> optsList = new ArrayList<Option>();
	    
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
	            } else {
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
	            break;
	        default:
	            // arg
	            break;
	        }
		}
		
		return optsList;
	}

	final MetaClientConfig metaClientConfig = new MetaClientConfig();
	final ZKConfig zkConfig = new ZKConfig();
	private DatabakConf conf;
	private MetadataStorage ms;
	private static ConcurrentLinkedQueue<DDLMsg> failedq = new ConcurrentLinkedQueue<DDLMsg>();
	
	public MsgConsumer(DatabakConf conf) {
		this.conf = conf;
		ms = new MetadataStorage(conf);
	}
	public void consume() throws MetaClientException {
		// 设置zookeeper地址
		zkConfig.zkConnect = conf.getZkaddr();
		metaClientConfig.setZkConfig(zkConfig);
		// New session factory,强烈建议使用单例
		MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
		final String topic = "meta-test";
		final String group = "meta-databak";
		// create consumer,强烈建议使用单例

		// 生成处理线程
		MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
		// 订阅事件，MessageListener是事件处理接口
		consumer.subscribe(topic, 1024, new MessageListener() {

			@Override
			public Executor getExecutor() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void recieveMessages(final Message message) {
				String data = new String(message.getData());
				int time = 0;
				System.out.println(data);
//				try {
//					Thread.sleep(10*1000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				DDLMsg msg = DDLMsg.fromJson(data);
				while(time < 3)
				{
					if(time >= 3)
					{
						failedq.add(msg);
						System.out.println("handle msg failed, add msg into failed queue: "+msg.getMsg_id());
						break;
					}
					try{
						ms.handleMsg(msg);
						if(failedq.size() > 0)
						{
							msg = failedq.poll();
							System.out.println("handle msg in failed queue: "+msg.getMsg_id());
							time = 0;
						}
						else 		//能到else一定是handlemsg没抛异常成功返回，而failedq是空的
							break;
					} catch (JedisConnectionException e) {
						time++;
						try {
							Thread.sleep(1*1000);
						} catch (InterruptedException e2) {
						}
						e.printStackTrace();
					} catch (IOException e) {
						time++;
						try {
							Thread.sleep(1*1000);
						} catch (InterruptedException e2) {
						}
						e.printStackTrace();
					} catch (NoSuchObjectException e) {
						time++;
						try {
							Thread.sleep(1*1000);
						} catch (InterruptedException e2) {
						}
						e.printStackTrace();
					} catch (TException e) {
						time++;
						try {
							Thread.sleep(1*1000);
						} catch (InterruptedException e2) {
						}
						e.printStackTrace();
					}
				}
					
				
			}

		});
		consumer.completeSubscribe();
	}

	
	public static void main(String[] args) {
		String addr = "192.168.1.13:3181";
		DatabakConf conf = null;
		if(args.length >= 1)
		{
			List<Option> ops = parseArgs(args);
			Set<String> sentinel = null;
			List<RedisInstance> ri = null;
			RedisMode rm = null;
			String zkaddr = null;
			String ra = null;
			String mh = null, mp = null;
			for (Option o : ops) {
				if (o.flag.equals("-h")) {
					// print help message
					System.out.println("-h    : print this help.");
					System.out.println("-mh   : metastore server ip.");
					System.out.println("-mp   : metastore server port.");
					System.out.println("-rm   : redis mode, <STA for stand alone or STL for sentinel>.");
					System.out.println("-ra   : redis or sentinel addr. <host:port;host:port> ");
					System.out.println("-zka  : zkaddr <host:port>.");
					
					System.exit(0);
				}
				if (o.flag.equals("-mh")) {
					// set serverPort
					if (o.opt == null) {
						System.out.println("-mh metastore server ip. ");
						System.exit(0);
					}
					mh = o.opt;
				}
				if (o.flag.equals("-mp")) {
					// set serverPort
					if (o.opt == null) {
						System.out.println("-mp metastore server port. ");
						System.exit(0);
					}
					mp = o.opt;
				}
				if (o.flag.equals("-rm")) {
					if (o.opt == null) {
						System.out.println("-rm redismode");
						System.exit(0);
					}
					if(o.opt.equals("STA"))
						rm = RedisMode.STANDALONE;
					else if(o.opt.equals("STL"))
						rm = RedisMode.SENTINEL;
					else{
						System.out.println("wrong redis mode:"+o.opt+", should be STA or STL");
						System.exit(0);
					}
				}
				if (o.flag.equals("-ra")) {
					// set serverPort
					if (o.opt == null) {
						System.out.println("-ra redis or sentinel addr. <host:port;host:port> ");
						System.exit(0);
					}
					ra = o.opt;
				}
				if (o.flag.equals("-zka")) {
	                // set redis server name
	                if (o.opt == null) {
	                        System.out.println("-zka zkaddr <host:port>.");
	                        System.exit(0);
	                }
	                zkaddr = o.opt;
		        }
			}
			
			if(mh == null || mp == null)
			{
				System.out.println("please provide ms host and ms port");
				System.exit(0);
			}
			
			if(rm == null || ra == null || zkaddr == null)
			{
				System.out.println("please provide enough args.");
				System.exit(0);
			}
			else{
				switch (rm){
				case SENTINEL:
					sentinel = new HashSet<String>();
					for(String s : ra.split(";"))
						sentinel.add(s);
					conf = new DatabakConf(sentinel, rm, zkaddr, mh, Integer.parseInt(mp));
					break;
				case STANDALONE:
					ri = new ArrayList<RedisInstance>();
					for(String rp : ra.split(";"))
					{
						System.out.println(rp);
						String[] s = rp.split(":");
						ri.add(new RedisInstance(s[0], Integer.parseInt(s[1])));
					}
					conf = new DatabakConf(ri, rm, zkaddr, mh, Integer.parseInt(mp));
					break;
				}
			}
			
		}
		else
		{
//			System.out.println("please provide arguments, use -h for help");
			List<RedisInstance> lr = new ArrayList<RedisInstance>();
			lr.add(new RedisInstance("localhost", 6379));
			conf = new DatabakConf(lr, RedisMode.STANDALONE, addr, "node13", 10101);
//			System.exit(0);
		}
		
		try {
//			System.setSecurityManager(new RMISecurityManager());
			DatabakRPC dr = new DatabakRPC(conf);
			Registry r = LocateRegistry.createRegistry(8111);
			r.rebind("DatabakRPC", dr);
			new MsgConsumer(conf).consume();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MetaClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
