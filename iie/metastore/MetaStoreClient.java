package iie.metastore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CreateOperation;
import org.apache.hadoop.hive.metastore.api.CreatePolicy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MSOperation;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.statfs;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

import devmap.DevMap;
import devmap.DevMap.DevStat;

public class MetaStoreClient {
	// client should be the local datacenter;
	public Database local_db;
	public IMetaStoreClient client;
	private ConcurrentHashMap<String, IMetaStoreClient> climap = new ConcurrentHashMap<String, IMetaStoreClient>();
	
	public static <T> T newInstance(Class<T> theClass,
			Class<?>[] parameterTypes, Object[] initargs) {
		// Perform some sanity checks on the arguments.
		if (parameterTypes.length != initargs.length) {
			throw new IllegalArgumentException(
					"Number of constructor parameter types doesn't match number of arguments");
		}
		for (int i = 0; i < parameterTypes.length; i++) {
			Class<?> clazz = parameterTypes[i];
			if (!(clazz.isInstance(initargs[i]))) {
				throw new IllegalArgumentException("Object : " + initargs[i]
						+ " is not an instance of " + clazz);
			}
		}

		try {
			Constructor<T> meth = theClass
					.getDeclaredConstructor(parameterTypes);
			meth.setAccessible(true);
			return meth.newInstance(initargs);
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate "
					+ theClass.getName(), e);
		}
	}

	public static class RetryingMetaStoreClient implements InvocationHandler {

		private final IMetaStoreClient base;
		private final int retryLimit;
		private final int retryDelaySeconds;

		protected RetryingMetaStoreClient(
				String msUri, int retry, int retryDelay, 
				HiveMetaHookLoader hookLoader,
				Class<? extends IMetaStoreClient> msClientClass)
				throws MetaException {
			this.retryLimit = retry;
			this.retryDelaySeconds = retryDelay;
			this.base = (IMetaStoreClient) newInstance(
					msClientClass, new Class[] { String.class, Integer.class, Integer.class, HiveMetaHookLoader.class }, 
					new Object[] { msUri, new Integer(retry), new Integer(retryDelay), hookLoader });
		}
		
		public static Class<?> getClass(String rawStoreClassName)
				throws MetaException {
			try {
				return Class.forName(rawStoreClassName, true,
						JavaUtils.getClassLoader());
			} catch (ClassNotFoundException e) {
				throw new MetaException(rawStoreClassName + " class not found");
			}
		}

		public static IMetaStoreClient getProxy(String msUri, int retry, int retryDelay, HiveMetaHookLoader hookLoader, String mscClassName)
				throws MetaException {

			Class<? extends IMetaStoreClient> baseClass = (Class<? extends IMetaStoreClient>) getClass(mscClassName);

			RetryingMetaStoreClient handler = new RetryingMetaStoreClient(msUri, retry, retryDelay, hookLoader, baseClass);

			return (IMetaStoreClient) Proxy.newProxyInstance(
					RetryingMetaStoreClient.class.getClassLoader(),
					baseClass.getInterfaces(), handler);
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			Object ret = null;
			int retriesMade = 0;
			TException caughtException = null;
			while (true) {
				try {
					ret = method.invoke(base, args);
					break;
				} catch (UndeclaredThrowableException e) {
					throw e.getCause();
				} catch (InvocationTargetException e) {
					if ((e.getCause() instanceof TApplicationException)
							|| (e.getCause() instanceof TProtocolException)
							|| (e.getCause() instanceof TTransportException)) {
						caughtException = (TException) e.getCause();
					} else if ((e.getCause() instanceof MetaException)
							&& e.getCause().getMessage()
									.matches("JDO[a-zA-Z]*Exception")) {
						caughtException = (MetaException) e.getCause();
					} else {
						throw e.getCause();
					}
				}

				if (retriesMade >= retryLimit) {
					throw caughtException;
				}
				retriesMade++;
				System.out.println(
						"MetaStoreClient lost connection. Attempting to reconnect." + 
						caughtException);
				Thread.sleep(retryDelaySeconds * 1000);
				base.reconnect();
			}
			return ret;
		}
	}
	
	public IMetaStoreClient createMetaStoreClient() throws MetaException {
		return createMetaStoreClient("localhost", 9083);
	}
	
	public IMetaStoreClient createMetaStoreClient(String uri) throws MetaException {
		HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
			public HiveMetaHook getHook(
					org.apache.hadoop.hive.metastore.api.Table tbl)
					throws MetaException {

				return null;
			}
		};
		return RetryingMetaStoreClient.getProxy(uri, 5, 1, hookLoader, HiveMetaStoreClient.class.getName());
	}

	public IMetaStoreClient createMetaStoreClient(String serverName, int port) throws MetaException {
		HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
			public HiveMetaHook getHook(
					org.apache.hadoop.hive.metastore.api.Table tbl)
					throws MetaException {

				return null;
			}
		};
		return RetryingMetaStoreClient.getProxy("thrift://" + serverName + ":" + port, 5, 1, hookLoader, HiveMetaStoreClient.class.getName());
	}
	
	public MetaStoreClient() throws MetaException {
		client = createMetaStoreClient();
		initmap(false);
	}
	
	public MetaStoreClient(String serverName, int port) throws MetaException {
		client = createMetaStoreClient(serverName, port);
		initmap(false);
	}

	public MetaStoreClient(String serverName, boolean preconnect) throws MetaException {
		client = createMetaStoreClient(serverName, 9083);
		initmap(preconnect);
	}
	
	public MetaStoreClient(String serverName) throws MetaException {
		client = createMetaStoreClient(serverName, 9083);
		initmap(false);
	}

	
	private void initmap(boolean preconnect) throws MetaException {
		// get local attribution
		try {
			this.local_db = client.get_local_attribution();
		} catch (TException e) {
			throw new MetaException(e.toString());
		}
		climap.put(local_db.getName(), client);
		
		// get all attributions
		List<Database> ld;
		try {
			ld = client.get_all_attributions();
		} catch (TException e) {
			throw new MetaException(e.toString());
		}
		for (Database db : ld) {
			if (!db.getName().equals(local_db.getName())) {
				if (preconnect) {
					System.out.println("Try to connect to Attribution " + db.getName() + ", uri=" + db.getParameters().get("service.metastore.uri"));
					try { 
						IMetaStoreClient cli = createMetaStoreClient(db.getParameters().get("service.metastore.uri"));
						climap.put(db.getName(), cli);
					} catch (MetaException me) {
						System.out.println("Connect to Datacenter " + db.getName() + ", uri=" + db.getParameters().get("service.metastore.uri") + " failed!");
					}
				}
			}
		}
	}
	
	public IMetaStoreClient getCli(String db_name) {
		IMetaStoreClient cli =  climap.get(db_name);
		if (cli == null) {
			// do reconnect now
			try {
				Database rdb = client.get_attribution(db_name);
				cli = createMetaStoreClient(rdb.getParameters().get("service.metastore.uri"));
				climap.put(db_name, cli);
			} catch (NoSuchObjectException e) {
				System.out.println(e);
			} catch (MetaException e) {
				System.out.println(e);
			} catch (TException e) {
				System.out.println(e);
			}
		}
		return cli;
	}
	
	public IMetaStoreClient getLocalCli() {
		return client;
	}
	
	public void stop() {
		if (client != null) {
			client.close();
		}
		for (Map.Entry<String, IMetaStoreClient> e : climap.entrySet()) {
			e.getValue().close();
		}
	}
	
	public static String splitValueToString(List<SplitValue> values) {
		String r = "", keys = "", vals = "";
		
		if (values == null)
			return "null";
		
		for (SplitValue sv : values) {
			keys += sv.getSplitKeyName() + ",";
			vals += "L:" + sv.getLevel() + ":V:" + sv.getVerison() + ":" + sv.getValue() + ",";
		}
		r += "KEYS [" + keys + "], VALS [" + vals + "]";
		
		return r;
	}
	
	public static String toStringSFile(SFile file) {
		if (file == null) {
			return "null";
		}
		
		String r = "<";
		r += "fid:" + file.getFid() + ", ";
		r += "db:" + file.getDbName() + ", ";
		r += "table:" + file.getTableName() + ", ";
		r += "status: " + file.getStore_status() + ", ";
		r += "repnr: " + file.getRep_nr() + ", ";
		r += "digest: " + file.getDigest() + ", ";
		r += "rec_nr: " + file.getRecord_nr() + ", ";
		r += "allrec_nr: " + file.getAll_record_nr() + ",";
		r += "len: " + file.getLength() + ",";
		r += "values: {" + splitValueToString(file.getValues()); 
		r += "}, [\n";
		if (file.getLocations() != null) {
			for (SFileLocation loc : file.getLocations()) {
				r += loc.getNode_name() + ":" + loc.getDevid() + ":"
						+ loc.getLocation() + ":" + loc.getRep_id() + ":"
						+ loc.getUpdate_time() + ":" + loc.getVisit_status()
						+ ":" + loc.getDigest() + "\n";
			}
		} else {
			r += "NULL";
		}
		r += "]>";
		
		return r;
	}
	
	public static class LFDThread extends Thread {
		private MetaStoreClient cli;
		public long begin, end;
		public String digest;
		public String line = "";
		public long fnr = 0;

		public LFDThread(MetaStoreClient cli, String digest) {
			this.cli = cli;
			this.digest = digest;
		}
		
		public void run() {
			try {
				long start = System.nanoTime();
				List<Long> files = cli.client.listFilesByDigest(digest);
				long stop = System.nanoTime();
				
				fnr = files.size();
				if (fnr > 0) {
					begin = System.nanoTime();
					for (Long fid : files) {
						SFile f = cli.client.get_file_by_id(fid);
						line += "fid " + f.getFid();
					}
					end = System.nanoTime();
					System.out.println(Thread.currentThread().getId() + "--> Search by digest consumed " + (stop - start) / 1000.0 + " us.");
					System.out.println(Thread.currentThread().getId() + "--> Get " + files.size() + " files in " + (end - begin) / 1000.0 + " us, GPS is " + files.size() * 1000000000.0 / (end - begin));
				}
			} catch (MetaException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class PingPongThread extends Thread {
		MetaStoreClient cli;
		public long begin, end;
		public long ppnr, pplen;
		
		public PingPongThread(MetaStoreClient cli, long ppnr, long pplen) {
			this.cli = cli;
			this.ppnr = ppnr;
			this.pplen = pplen;
		}
		
		public void run() {
			StringBuffer sb = new StringBuffer();
			
    		for (int i = 0; i < pplen; i++) {
    			sb.append(Integer.toHexString(i).charAt(0));
    		}
    		begin = System.nanoTime();
    		try {
    			for (int i = 0; i < ppnr; i++) {
    				cli.client.pingPong(sb.toString());
    			}
    		} catch (MetaException e) {
    			e.printStackTrace();
    		} catch (TException e) {
    			e.printStackTrace();
    		}
    		end = System.nanoTime();
		}
	}
	
	public static boolean runRemoteCmd(String cmd) throws IOException {
		Process p = Runtime.getRuntime().exec(new String[] {"/bin/bash", "-c", cmd});
		try {
			InputStream err = p.getErrorStream();
			InputStreamReader isr = new InputStreamReader(err);
			BufferedReader br = new BufferedReader(isr);

			String line = null;

			System.out.println("<ERROR>");

			while ((line = br.readLine()) != null)

				System.out.println(line);
			System.out.println("</ERROR>");

			int exitVal = p.waitFor();
			System.out.println(" -> exit w/ " + exitVal);
			if (exitVal > 0)
				return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public static String runRemoteCmdWithResult(String cmd) throws IOException {
		Process p = Runtime.getRuntime().exec(new String[] {"/bin/bash", "-c", cmd});
		String result = "";
		
		try {
			InputStream err = p.getErrorStream();
			InputStreamReader isr = new InputStreamReader(err);
			BufferedReader br = new BufferedReader(isr);

			String line = null;

			System.out.println("<ERROR>");

			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
			System.out.println("</ERROR>");
			
			InputStream out = p.getInputStream();
			isr = new InputStreamReader(out);
			br = new BufferedReader(isr);

			System.out.println("<OUTPUT>");

			while ((line = br.readLine()) != null) {
				result += line;
				System.out.println(line);
			}
			System.out.println("</OUTPUT>");

			int exitVal = p.waitFor();
			System.out.println(" -> exit w/ " + exitVal);
			if (exitVal > 0)
				return result;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	
	public static void main(String[] args) throws IOException {
		MetaStoreClient cli = null;
		String node = null;
		String serverName = null;
		int serverPort = 9083;
		List<String> ipl = new ArrayList<String>();
		int repnr = 3;
		SFile file = null, r = null;
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    String dbName = null, tableName = null, partName = null, to_dc = null, to_db = null, to_nas_devid = null,
	    		tunnel_in = null, tunnel_out = null, tunnel_node = null, tunnel_user = null;
	    int prop = 0, pplen = 0, ppnr = 1, ppthread = 1, lfdc_thread = 1;
	    String devid = null;
	    String node_name = null;
	    String sap_key = null, sap_value = null;
	    String flt_l1_key = null, flt_l1_value = null, flt_l2_key = null, flt_l2_value = null;
	    int flctc_nr = 0;
	    String digest = "";
	    boolean lfd_verbose = false;
	    long begin_time = -1, end_time = -1;
	    String ANSI_RESET = "\u001B[0m";
	    String ANSI_RED = "\u001B[31m";
	    String ANSI_GREEN = "\u001B[32m";
	    long ofl_fid = -1;
	    String ofl_sfl_dev = null;
	    
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
	                    	optsList.add(new MetaStoreClient.Option(args[i], args[i+1]));
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
	    		System.out.println("-h   : print this help.");
	    		System.out.println("-r   : server name.");
	    		System.out.println("-p   : server port.");
	    		System.out.println("-n   : add current machine as a new node.");
	    		System.out.println("-f   : auto test file operations, from create to delete.");
	    		System.out.println("-sd  : show device");
	    		System.out.println("-md  : modify device: change prop or attached node.");
	    		System.out.println("-cd  : add new device.");
	    		System.out.println("-dd  : delete device.");
	    		System.out.println("-ld  : list existing devices.");
	    		System.out.println("-nn  : add node with specified name.");
	    		System.out.println("-dn  : delete node.");
	    		System.out.println("-ln  : list existing node.");
	    		System.out.println("-gni : get current active Node Info from DM.");
	    		System.out.println("-dms : get current DM status.");
	    		System.out.println("-frr : read the file object by fid.");
	    		System.out.println("-sap : set attribution parameters.");
	    		System.out.println("-lst : list table files.");
	    		System.out.println("-lfd : list files by digest.");
	    		System.out.println("-flt : filter table files.");
	    		System.out.println("-tct : truncate table files.");
	    		System.out.println("-pp  : ping pong latency test.");
	    		System.out.println("-flctc : lots of file createtion test.");
	    		System.out.println("-lfdc: concurrent list files by digest test.");
	    		System.out.println("-fro : reopen a file.");
	    		System.out.println("-cvt : convert date to timestamp.");

	    		System.out.println("");
	    		System.out.println("Be careful with following operations!");
	    		System.out.println("");
	    		
	    		System.out.println("-tsm : toggle safe mode of DM.");
	    		System.out.println("-fcr : create a new file and return the fid.");
	    		System.out.println("-fcl : close the file.");
	    		System.out.println("-fcd : delete the file.");
	    		
	    		System.exit(0);
	    	}
	    	if (o.flag.equals("-r")) {
	    		// set servername;
	    		serverName = o.opt;
	    	}
	    	if (o.flag.equals("-p")) {
	    		// set serverPort
	    		serverPort = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-prop")) {
	    		// device prop
	    		prop = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-devid")) {
	    		// device ID
	    		devid = o.opt;
	    	}
	    	if (o.flag.equals("-node")) {
	    		// node name for device creation
	    		node_name = o.opt;
	    	}
	    	if (o.flag.equals("-table")) {
	    		// set table name
	    		if (o.opt == null) {
	    			System.out.println("-table tableName");
	    			System.exit(0);
	    		}
	    		tableName = o.opt;
	    	}
	    	if (o.flag.equals("-db")) {
	    		// set db name
	    		if (o.opt == null) {
	    			System.out.println("-db dbName");
	    			System.exit(0);
	    		}
	    		dbName = o.opt;
	    	}
	    	if (o.flag.equals("-part")) {
	    		// set part name
	    		if (o.opt == null) {
	    			System.out.println("-part partName");
	    			System.exit(0);
	    		}
	    		partName = o.opt;
	    	}
	    	if (o.flag.equals("-todc")) {
	    		// set to_dc name
	    		if (o.opt == null) {
	    			System.out.println("-todc target_dc");
	    			System.exit(0);
	    		}
	    		to_dc = o.opt;
	    	}
	    	if (o.flag.equals("-todb")) {
	    		// set to_db name
	    		if (o.opt == null) {
	    			System.out.println("-todb target_db");
	    			System.exit(0);
	    		}
	    		to_db = o.opt;
	    	}
	    	if (o.flag.equals("-tonasdev")) {
	    		// set to_nas_devid name
	    		if (o.opt == null) {
	    			System.out.println("-tonasdev NAS_DEVID");
	    			System.exit(0);
	    		}
	    		to_nas_devid = o.opt;
	    	}
	    	if (o.flag.equals("-tunnel_in")) {
	    		// set tunnel name
	    		if (o.opt == null) {
	    			System.out.println("-tunnel_in TUNNEL_IN_PATH");
	    			System.exit(0);
	    		}
	    		tunnel_in = o.opt;
	    	}
	    	if (o.flag.equals("-tunnel_out")) {
	    		// set tunnel_out name
	    		if (o.opt == null) {
	    			System.out.println("-tunnel_out TUNNEL_OUT_PATH");
	    			System.exit(0);
	    		}
	    		tunnel_out = o.opt;
	    	}
	    	if (o.flag.equals("-tunnel_node")) {
	    		// set tunnel_node name
	    		if (o.opt == null) {
	    			System.out.println("-tunnel_node TUNNEL_NODE_NAME");
	    			System.exit(0);
	    		}
	    		tunnel_node = o.opt;
	    	}
	    	if (o.flag.equals("-tunnel_user")) {
	    		// set tunnel_user name
	    		if (o.opt == null) {
	    			System.out.println("-tunnel_user TUNNEL_USER_NAME");
	    			System.exit(0);
	    		}
	    		tunnel_user = o.opt;
	    	}
	    	if (o.flag.equals("-sap_key")) {
	    		// set sap key
	    		if (o.opt == null) {
	    			System.out.println("-sap_key ATTRIBUTION_KEY");
	    			System.exit(0);
	    		}
	    		sap_key = o.opt;
	    	}
	    	if (o.flag.equals("-sap_value")) {
	    		// set sap value
	    		if (o.opt == null) {
	    			System.out.println("-sap_value ATTRIBUTION_VALUE");
	    			System.exit(0);
	    		}
	    		sap_value = o.opt;
	    	}
	    	if (o.flag.equals("-flt_l1_key")) {
	    		// set filter table level_1 key
	    		if (o.opt == null) {
	    			System.out.println("-flt_l1_key level 1 pkey");
	    			System.exit(0);
	    		}
	    		flt_l1_key = o.opt;
	    	}
	    	if (o.flag.equals("-flt_l1_value")) {
	    		// set filter table level_1 value
	    		if (o.opt == null) {
	    			System.out.println("-flt_l1_key level 1 value");
	    			System.exit(0);
	    		}
	    		flt_l1_value = o.opt;
	    	}
	    	if (o.flag.equals("-flt_l2_key")) {
	    		// set filter table level_2 key
	    		if (o.opt == null) {
	    			System.out.println("-flt_l2_key level 2 pkey");
	    			System.exit(0);
	    		}
	    		flt_l2_key = o.opt;
	    	}
	    	if (o.flag.equals("-flt_l2_value")) {
	    		// set filter table level_2 value
	    		if (o.opt == null) {
	    			System.out.println("-flt_l2_value level 2 value");
	    			System.exit(0);
	    		}
	    		flt_l2_value = o.opt;
	    	}
	    	if (o.flag.equals("-pplen")) {
	    		// set ping pong string length
	    		if (o.opt == null) {
	    			System.out.println("-pplen length");
	    			System.exit(0);
	    		}
	    		pplen = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-ppnr")) {
	    		// set ping pong number
	    		if (o.opt == null) {
	    			System.out.println("-ppnr number");
	    			System.exit(0);
	    		}
	    		ppnr = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-ppthread")) {
	    		// set ping pong thread number
	    		if (o.opt == null) {
	    			System.out.println("-ppthread number");
	    			System.exit(0);
	    		}
	    		ppthread = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-flctc_nr")) {
	    		// set lots of files number
	    		if (o.opt == null) {
	    			System.out.println("-flctc_nr number");
	    			System.exit(0);
	    		}
	    		flctc_nr = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-lfd_digest")) {
	    		// set digest string
	    		if (o.opt == null) {
	    			System.out.println("-lfd_digest STRING");
	    			System.exit(0);
	    		}
	    		digest = o.opt;
	    	}
	    	if (o.flag.equals("-lfd_verbose")) {
	    		// set LFD verbose flag
	    		if (o.opt == null) {
	    			System.out.println("-lfd_verbose");
	    			System.exit(0);
	    		}
	    		lfd_verbose = true;
	    	}
	    	if (o.flag.equals("-lfdc_thread")) {
	    		// set LFD thread number
	    		if (o.opt == null) {
	    			System.out.println("-lfdc_thread number");
	    			System.exit(0);
	    		}
	    		lfdc_thread = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-begin_time")) {
	    		// set begin_time
	    		if (o.opt == null) {
	    			System.out.println("-begin_time timestamp");
	    			System.exit(0);
	    		}
	    		begin_time = Long.parseLong(o.opt);
	    	}
	    	if (o.flag.equals("-end_time")) {
	    		// set end time
	    		if (o.opt == null) {
	    			System.out.println("-end_time timestamp");
	    			System.exit(0);
	    		}
	    		end_time = Long.parseLong(o.opt);
	    	}
	    }
	    if (cli == null) {
	    	try {
	    		if (serverName == null)
	    			cli = new MetaStoreClient();
	    		else
	    			cli = new MetaStoreClient(serverName, serverPort);
			} catch (MetaException e) {
				e.printStackTrace();
				System.exit(0);
			}
	    }
	    try {
			node = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}

	    for (Option o : optsList) {
	    	if (o.flag.equals("-pp")) {
	    		// ping pong test
	    		long tppnr;
	    		
	    		tppnr = ppnr / ppthread * ppthread;
	    		List<PingPongThread> ppts = new ArrayList<PingPongThread>();
	    		for (int i = 0; i < ppthread; i++) {
	    			MetaStoreClient tcli = null;
	    			
	    			if (serverName == null)
						try {
							tcli = new MetaStoreClient();
						} catch (MetaException e) {
							e.printStackTrace();
							System.exit(0);
						}
					else
						try {
							tcli = new MetaStoreClient(serverName, serverPort);
						} catch (MetaException e) {
							e.printStackTrace();
							System.exit(0);
						}
	    			ppts.add(new PingPongThread(tcli, tppnr, pplen));
	    		}
	    		long begin = System.nanoTime();
	    		for (PingPongThread t : ppts) {
	    			t.start();
	    		}
	    		for (PingPongThread t : ppts) {
	    			try {
						t.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
	    		}
	    		long end = System.nanoTime();
	    		long tp = 0;
	    		for (PingPongThread t : ppts) {
	    			tp += t.ppnr / ((end - begin) / 1000000000.0);
	    		}
	    		System.out.println("PingPong: thread " + ppthread + " nr " + tppnr + " len " + pplen + 
	    				" avg Latency " + (end - begin) / tppnr / 1000.0 + " us, ThroughPut " + tp + ".");
	    	}
	    	if (o.flag.equals("-authtest")) {
	    		// auth test
	    		User user = new User("macan", "111111", System.currentTimeMillis(), "root");
	    		List<MSOperation> ops = new ArrayList<MSOperation>();
	    		
	    		ops.add(MSOperation.CREATETABLE);
	    		try {
					Table tbl = cli.client.getTable("db1", "pokes");
					System.out.println("AUTH CHECK: " + cli.client.user_authority_check(user, tbl, ops));
				} catch (AlreadyExistsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidObjectException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MetaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NoSuchObjectException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

	    	}
	    	if (o.flag.equals("-m")) {
	    		// migrate
	    		if (dbName == null || tableName == null || partName == null || to_dc == null) {
	    			System.out.println("Please set dbname,tableName,partName,to_dc!");
	    			System.exit(0);
	    		}
	    	}
	    	if (o.flag.equals("-m21")) {
	    		// migrate by NAS stage 1
	    		if (dbName == null || tableName == null || partName == null || to_dc == null || to_nas_devid == null || tunnel_in == null ||
	    				tunnel_out == null || tunnel_node == null) {
	    			System.out.println("Please set dbname,tableName,partName,to_dc,tonasdev,tunnel_in,tunnel_out,tunnel_node!");
	    			System.exit(0);
	    		}
	    		if (tunnel_user == null) {
	    			System.out.println("#Copy using default user, this might not be your purpose.");
	    		}
	    		List<String> partNames = new ArrayList<String>();
	    		partNames.add(partName);
	    		try {
	    			List<SFileLocation> lsfl = cli.client.migrate2_stage1(dbName, tableName, partNames, to_dc);
	    			//SFileLocation t = new SFileLocation("", 90, "devid", "/data/default/swjl/1031676804", 9, 1000, 1, "digest");
	    			//lsfl.add(t);
	    			if (lsfl.size() > 0) {
	    				// copy NAS or non-NAS LOC to TUNNEL 
	    				for (SFileLocation sfl : lsfl) {
	    					if (sfl.getNode_name().equals("")) {
	    						System.out.println("#Get NAS LOC " + sfl.getDevid() + ":" + sfl.getLocation() + " : " + sfl.getDigest());
	    					} else {
	    						System.out.println("#Get non-NAS LOC " + sfl.getDevid() + ":" + sfl.getLocation());
	    					}
	    					// calculate the source path: tunnel_in + location
	    					String sourceFile = tunnel_in + sfl.getLocation();
	    					// calculate the target path
	    					String targetFile = tunnel_out + sfl.getLocation();
	    					// create the target directory now
	    					File tf = new File(targetFile);
	    					System.out.println("#Create parent DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    					String cmd = "mkdir -p " + tf.getParent() + "; " + "chmod ugo+rw " + tf.getParent() + ";";
	    					System.out.println(cmd);
	    						    					
	    					// do copy now
	    					if (sfl.getNode_name().equals("")) {
	    						System.out.println("#Copy     NAS SFL by TUNNEL: " + sourceFile + " -> " + tf.getParent());
	    						cmd = "cp -r " + sourceFile + " " + tf.getParent() + "; " + "chmod -R ugo+rw " + targetFile + ";";
	    						System.out.println(cmd);
	    						cmd = "find " + targetFile + " -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';";
	    						System.out.println(cmd);
	    					} else {
	    						// reset sourceFile
	    						sourceFile = cli.client.getMP(sfl.getNode_name(), sfl.getDevid()) + "/" + sfl.getLocation();
	    						System.out.println("#Copy non-NAS SFL by TUNNEL: " + sourceFile + " -> " + tf.getParent());
	    						cmd = "scp -r metastore@" + sfl.getNode_name() + ":" + sourceFile + " " + tf.getParent() + "; " + "chmod -R ugo+rw " + targetFile + ";";
	    						System.out.println(cmd);
	    						cmd = "find " + targetFile + " -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';";
	    						System.out.println(cmd);
	    					}
	    				}
	    			} else {
	    				System.out.println("No data to migrate");
	    			}
	    		} catch (MetaException me) {
	    			me.printStackTrace();
	    			break;
	    		} catch (TException e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-m22")) {
	    		// migrate by NAS
	    		if (dbName == null || tableName == null || partName == null || to_dc == null || to_nas_devid == null || tunnel_in == null ||
	    				tunnel_out == null || tunnel_node == null) {
	    			System.out.println("Please set dbname,tableName,partName,to_dc,tonasdev,tunnel_in,tunnel_out,tunnel_node!");
	    			System.exit(0);
	    		}
	    		List<String> partNames = new ArrayList<String>();
	    		partNames.add(partName);
	    		try {
	    			List<SFileLocation> lsfl = cli.client.migrate2_stage1(dbName, tableName, partNames, to_dc);
	    			if (lsfl.size() > 0) {
	    				// migrate by NAS stage2
	    				if (cli.client.migrate2_stage2(dbName, tableName, partNames, to_dc, to_db, to_nas_devid)) {
	    					System.out.println("Migrate2 Stage2 Done.");
	    				} else 
	    					System.out.println("Migrate2 Stage2 Failed.");
	    			} else {
	    				System.out.println("No data to migrate");
	    			}
	    		} catch (MetaException me) {
	    			me.printStackTrace();
	    			break;
	    		} catch (TException e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-m2")) {
	    		// migrate by NAS
	    		if (dbName == null || tableName == null || partName == null || to_dc == null || to_nas_devid == null || tunnel_in == null ||
	    				tunnel_out == null || tunnel_node == null) {
	    			System.out.println("Please set dbname,tableName,partName,to_dc,tonasdev,tunnel_in,tunnel_out,tunnel_node!");
	    			System.exit(0);
	    		}
	    		if (tunnel_user == null) {
	    			System.out.println("Copy using default user, this might not be your purpose.");
	    		}
	    		List<String> partNames = new ArrayList<String>();
	    		partNames.add(partName);
	    		try {
	    			List<SFileLocation> lsfl = cli.client.migrate2_stage1(dbName, tableName, partNames, to_dc);
	    			//SFileLocation t = new SFileLocation("", 90, "devid", "/data/default/swjl/1031676804", 9, 1000, 1, "digest");
	    			//lsfl.add(t);
	    			if (lsfl.size() > 0) {
	    				// copy NAS or non-NAS LOC to TUNNEL 
	    				for (SFileLocation sfl : lsfl) {
	    					if (sfl.getNode_name().equals("")) {
	    						System.out.println("Get NAS LOC " + sfl.getDevid() + ":" + sfl.getLocation() + " : " + sfl.getDigest());
	    					} else {
	    						System.out.println("Get non-NAS LOC " + sfl.getDevid() + ":" + sfl.getLocation());
	    					}
	    					// calculate the source path: tunnel_in + location
	    					String sourceFile = tunnel_in + sfl.getLocation();
	    					// calculate the target path
	    					String targetFile = tunnel_out + sfl.getLocation();
	    					// create the target directory now
	    					File tf = new File(targetFile);
	    					System.out.println("Create parent DIR " + "@" + tunnel_node + ": " + tf.getParent());
	    					String cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node + " 'mkdir -p " + tf.getParent() + "; " + "chmod ugo+rw " + tf.getParent() + ";'";
	    					System.out.println(cmd);
	    					if (!runRemoteCmd(cmd)) {
	    						System.exit(1);
	    					}
	    						    					
	    					// do copy now
	    					if (sfl.getNode_name().equals("")) {
	    						System.out.println("Copy     NAS SFL by TUNNEL: " + sourceFile + " -> " + tf.getParent());
	    						cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node + " 'cp -r " + sourceFile + " " + tf.getParent() + "; " + "chmod -R ugo+rw " + targetFile + ";'";
	    						System.out.println(cmd);
	    						if (!runRemoteCmd(cmd)) {
	    							System.exit(1);
	    						}
	    						
	    						cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node;
	    						cmd += " find " + targetFile + " -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';";
	    						System.out.println(cmd);
	    						String md5 = runRemoteCmdWithResult(cmd);
	    						if (!sfl.getDigest().equalsIgnoreCase(md5) && !sfl.getDigest().equals("MIGRATE2-DIGESTED!") && !sfl.getDigest().equals("REMOTE-DIGESTED!") && !sfl.getDigest().equals("SFL_DEFAULT")) {
	    							System.out.println("MD5 mismatch: original MD5 " + sfl.getDigest() + ", target MD5 " + md5 + ".");
	    							System.exit(1);
	    						}
	    					} else {
	    						// reset sourceFile
	    						sourceFile = cli.client.getMP(sfl.getNode_name(), sfl.getDevid()) + "/" + sfl.getLocation();
	    						System.out.println("Copy non-NAS SFL by TUNNEL: " + sourceFile + " -> " + tf.getParent());
	    						cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node + " 'scp -r metastore@" + sfl.getNode_name() + ":" + sourceFile + " " + tf.getParent() + "; " + "chmod -R ugo+rw " + targetFile + ";'";
	    						System.out.println(cmd);
	    						if (!runRemoteCmd(cmd)) {
	    							System.exit(1);
	    						}
	    						
	    						cmd = "ssh " + (tunnel_user == null ? "" : tunnel_user + "@") + tunnel_node;
	    						cmd += " \"find " + targetFile + " -type f -exec md5sum {} + | awk '{print $1}' | sort | md5sum | awk '{print $1}';\"";
	    						System.out.println(cmd);
	    						String md5 = runRemoteCmdWithResult(cmd);
	    						if (!sfl.getDigest().equalsIgnoreCase(md5) && !sfl.getDigest().equals("MIGRATE2-DIGESTED!") && !sfl.getDigest().equals("REMOTE-DIGESTED!") && !sfl.getDigest().equals("SFL_DEFAULT")) {
	    							System.out.println("MD5 mismatch: original MD5 " + sfl.getDigest() + ", target MD5 " + md5 + ".");
	    							System.exit(1);
	    						}
	    					}
	    				}
	    				// begin stage2
	    				if (cli.client.migrate2_stage2(dbName, tableName, partNames, to_dc, to_db, to_nas_devid)) {
	    					System.out.println("Migrate2 Stage2 Done.");
	    				} else 
	    					System.out.println("Migrate2 Stage2 Failed.");
	    			} else {
	    				System.out.println("No data to migrate");
	    			}
	    		} catch (MetaException me) {
	    			me.printStackTrace();
	    			break;
	    		} catch (TException e) {
	    			e.printStackTrace();
	    			break;
	    		}
	    	}
	    	if (o.flag.equals("-ld")) {
	    		// list device
	    		List<Device> ds;
				try {
					ds = cli.client.listDevice();
					if (ds.size() > 0) {
		    			for (Device d : ds) {
		    				System.out.println("-node " + d.getNode_name() + " -devid " + d.getDevid() + " -prop " + d.getProp());
		    			}
					}
				} catch (MetaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	}
	    	if (o.flag.equals("-sd")) {
	    		// show device
	    		if (devid == null) {
	    			System.out.println("Please set -devid");
	    			System.exit(0);
	    		}
	    		try {
					Device d = cli.client.getDevice(devid);
					String sprop, status;
					switch (d.getProp()) {
					case MetaStoreConst.MDeviceProp.ALONE:
						sprop = "ALONE";
						break;
					case MetaStoreConst.MDeviceProp.SHARED:
						sprop = "SHARED";
						break;
					case MetaStoreConst.MDeviceProp.BACKUP:
						sprop = "BACKUP";
						break;
					case MetaStoreConst.MDeviceProp.BACKUP_ALONE:
						sprop = "BACKUP_ALONE";
						break;
					default:
						sprop = "Unknown";
					}
					switch (d.getStatus()) {
					case MetaStoreConst.MDeviceStatus.ONLINE:
						status = "ONLINE";
						break;
					case MetaStoreConst.MDeviceStatus.OFFLINE:
						status = "OFFLINE";
						break;
					case MetaStoreConst.MDeviceStatus.SUSPECT:
						status = "SUSPECT";
						break;
					default:
						status = "Unknown";
					}
					System.out.println("Device -> [" + d.getNode_name() + ":" + d.getDevid() + ":" + sprop + ":" + status + "]");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-md")) {
	    		// modify Device
	    		if (node_name == null || devid == null) {
	    			System.out.println("Please set -node -prop -devid.");
	    			System.exit(0);
	    		}
	    		try {
	    			Node n = cli.client.get_node(node_name);
					Device d = cli.client.getDevice(devid);
					d.setProp(prop);
					cli.client.changeDeviceLocation(d, n);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-cd")) {
	    		// add Device
	    		if (node_name == null) {
	    			System.out.println("Please set -node -prop.");
	    			System.exit(0);
	    		}
	    		try {
					Device d = cli.client.createDevice(o.opt, prop, node_name);
					System.out.println("Add Device: " + d.getDevid() + ", prop " + d.getProp() + ", node " + d.getNode_name());
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
	    	if (o.flag.equals("-dd")) {
	    		// del Device
	    		try {
					cli.client.delDevice(o.opt);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
	    	}
			if (o.flag.equals("-n")) {
				// add Node
				try {
					ipl.add(InetAddress.getLocalHost().getHostAddress());
					System.out.println("Add Node: " + node + ", IPL: " + ipl.get(0));
					Node n = cli.client.add_node(node, ipl);
				} catch (MetaException me) {
					me.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					return;
				} catch (UnknownHostException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-nn")) {
				// add Node with specified name
				if (o.opt != null) {
					try {
						ipl.add(InetAddress.getLocalHost().getHostAddress());
						System.out.println("Add Node: " + o.opt + ", IPL: " + ipl.get(0));
						Node n = cli.client.add_node(o.opt, ipl);
					} catch (UnknownHostException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (MetaException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			if (o.flag.equals("-dn")) {
				// del Node
				try {
					cli.client.del_node(o.opt);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-ln")) {
				// list all Nodes
				List<Node> lns;
				try {
					lns = cli.client.get_all_nodes();
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				for (Node n : lns) {
					System.out.println("Node '" + n.getNode_name() + "' {" + n.getIps().toString() + "}");
				}
			}
			if (o.flag.equals("-gni")) {
				// get NodeInfo 
				String nis;
				try {
					nis = cli.client.getNodeInfo();
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				System.out.println(nis);
			}
			if (o.flag.equals("-dms")) {
				// get DM status
				String dms;
				try {
					dms = cli.client.getDMStatus();
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
				System.out.println(dms);
			}
			if (o.flag.equals("-statfs")) {
				// stat the file system
				if (begin_time < 0 || end_time < 0) {
					System.out.println("Please set -begin_time and -end_time");
					System.exit(0);
				}
				try {
					statfs s = cli.client.statFileSystem(begin_time, end_time);
					System.out.println("Query on time range [" + begin_time + "," + end_time + ") {" +
							new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(begin_time * 1000)) + "," + 
							new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(end_time * 1000)) + 
							"} -> ");
					System.out.println(" INCREATE     " + s.getIncreate());
					System.out.println(" CLOSE        " + s.getClose());
					System.out.println(" REPLICATED   " + s.getReplicated());
					System.out.println(" RM_LOGICAL   " + s.getRm_logical());
					System.out.println(" RM_PHYSICAL  " + s.getRm_physical());
					System.out.println("");
					System.out.println(" OVERREP      " + s.getOverrep());
					System.out.println(" UNDERREP     " + s.getUnderrep());
					System.out.println(" LINGER       " + s.getLinger());
					System.out.println(" SUSPECT      " + s.getSuspect());
					System.out.println("");
					System.out.println(" INC OFFLINE  " + (s.getIncreate() - s.getInc_ons() - s.getInc_ons2()));
					System.out.println(" INC ONLINE   " + s.getInc_ons());
					System.out.println(" INC ONLINE2+ " + ANSI_RED + s.getInc_ons2() + ANSI_RESET);
					System.out.println(" CLS OFFLINE  " + ANSI_RED + s.getCls_offs() + ANSI_RESET);
					System.out.println("");
					System.out.println(" COLS         " + s.getClos());
					System.out.println(" INCS O2ERR   " + s.getIncs());
					System.out.println("");
					System.out.println(" File in Tab  " + s.getFnrs());
					System.out.println("");
					if (s.getIncsSize() > 0) {
						System.out.println(ANSI_RED + "BAD STATE in Our Store! Please notify <macan@iie.ac.cn>" + ANSI_RESET);
					} else {
						System.out.println(ANSI_GREEN + "GOOD STATE ;)");
					}
				} catch (MetaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-ofl")) {
				// offline a file location
				if (ofl_fid < 0 || ofl_sfl_dev == null) {
					System.out.println("Please set -ofl_fid and -ofl_sfl_dev.");
					System.exit(0);
				}
				SFile f;
				try {
					f = cli.client.get_file_by_id(ofl_fid);
					SFileLocation sfl = null;
				
					if (f.getLocationsSize() > 0) {
						for (SFileLocation fl : f.getLocations()) {
							if (fl.getDevid().equalsIgnoreCase(ofl_sfl_dev)) {
								sfl = fl;
								break;
							}
						}
					}
					if (sfl != null)
						cli.client.offline_filelocation(sfl);
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MetaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-tsm")) {
				// toggle safe mode, do NOT use it unless you know what are you doing
				try {
					System.out.println("Toggle Safe Mode: " + cli.client.toggle_safemode());
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-sap")) {
				// set attribution kv parameter
				if (sap_key == null || sap_value == null) {
					System.out.println("Please set sap_key and sap_value.");
					System.exit(0);
				}
				Database db;
				try {
					db = cli.client.get_local_attribution();
					Map<String, String> nmap = db.getParameters();
					nmap.put(sap_key, sap_value);
					db.setParameters(nmap);
					cli.client.alterDatabase(db.getName(), db);
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-flt")) {
				// filter table files
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table");
					System.exit(0);
				}
				List<SplitValue> values = new ArrayList<SplitValue>();
				if (flt_l1_key != null && flt_l1_value != null) {
					// split value into many sub values
					String[] l1vs = flt_l1_value.split(";");
					for (String vs : l1vs) {
						values.add(new SplitValue(flt_l1_key, 1, vs, 0));
					}
					if (flt_l2_key != null && flt_l2_value != null) {
						String[] l2vs = flt_l2_value.split(";");
						for (String vs : l2vs) {
							values.add(new SplitValue(flt_l2_key, 2, vs, 0));
						}
					}
				}
				try {
					List<SFile> files = cli.client.filterTableFiles(dbName, tableName, values);
					for (SFile f : files) {
						System.out.println("fid " + f.getFid() + " -> " + toStringSFile(f));
					}
					System.out.println("Total " + files.size() + " file(s) listed.");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-tct")) {
				// trunc table files
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table.");
					System.exit(0);
				}
				try {
					cli.client.truncTableFiles(dbName, tableName);
					System.out.println("Begin backgroud table truncate now, please wait!");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-lfd")) {
				// list files by digest
				System.out.println("Please use -lfd_digest to change digest string; -lfd_verbose to dump more file info.");
				try {
					long start = System.nanoTime();
					List<Long> files = cli.client.listFilesByDigest(digest);
					long stop = System.nanoTime();
					if (files.size() > 0) {
						long begin = System.nanoTime();
						for (Long fid : files) {
							SFile f = cli.client.get_file_by_id(fid);
							String line = "fid " + fid;
							if (lfd_verbose) {
								line += " -> " + toStringSFile(f);
							}
							System.out.println(line);
						}
						long end = System.nanoTime();
						System.out.println("--> Search by digest consumed " + (stop - start) / 1000.0 + " us.");
						System.out.println("--> Get " + files.size() + " files in " + (end - begin) / 1000.0 + " us, GPS is " + files.size() * 1000000000.0 / (end - begin));
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-lfdc")) {
				// list files by digest with concurrent mgets
				System.out.println("ONLY USED for TEST. Set lfdc_thread to thread number.");
				List<LFDThread> lfdts = new ArrayList<LFDThread>();
				for (int i = 0; i < lfdc_thread; i++) {
					MetaStoreClient tcli = null;
	    			
	    			if (serverName == null)
						try {
							tcli = new MetaStoreClient();
						} catch (MetaException e) {
							e.printStackTrace();
							System.exit(0);
						}
					else
						try {
							tcli = new MetaStoreClient(serverName, serverPort);
						} catch (MetaException e) {
							e.printStackTrace();
							System.exit(0);
						}
	    			lfdts.add(new LFDThread(tcli, digest));
				}
				for (LFDThread t : lfdts) {
					t.start();
				}
				for (LFDThread t : lfdts) {
					try {
						t.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				long tfnr = 0, tgps = 0;
				for (LFDThread t : lfdts) {
					tfnr += t.fnr;
					tgps += t.fnr / ((t.end - t.begin) / 1000000000.0);
				}
				System.out.println("LFDCON: thread " + lfdc_thread + " total got " + tfnr + 
						" files, total GPS " + tgps);
			}
			if (o.flag.equals("-ltg")) {
				// list table groups
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table.");
					System.exit(0);
				}
				try {
					List<NodeGroup> ngs = cli.client.getTableNodeGroups(dbName, tableName);
					for (NodeGroup ng : ngs) {
						System.out.println("NG: " + ng.getNode_group_name() + " -> {");
						if (ng.getNodesSize() > 0) {
							for (Node n : ng.getNodes()) {
								System.out.println(" Node " + n.getNode_name());
							}
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-lst")) {
				// list table files
				if (dbName == null || tableName == null) {
					System.out.println("please set -db and -table.");
					System.exit(0);
				}
				try {
					long size = 0;
					for (int i = 0; i < Integer.MAX_VALUE; i += 1000) {
					List<Long> files = cli.client.listTableFiles(dbName, tableName, i, i + 1000);
						if (files.size() > 0) {
							for (Long fid : files) {
								SFile f = cli.client.get_file_by_id(fid);
								System.out.println("fid " + fid + " -> " + toStringSFile(f));
							}
						}
						size += files.size();
						if (files.size() == 0)
							break;
					}
					System.out.println("Total " + size + " file(s) listed.");
				} catch (MetaException e) {
					e.printStackTrace();
					break;
				} catch (TException e) {
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-flctc")) {
				// create lots of files, touch it, close it to do pressure-test
				System.out.println("Please use -flctc_nr to set file numbers, default is " + flctc_nr + ".");
				List<SplitValue> values = new ArrayList<SplitValue>();
				DevMap dm = new DevMap();
				
				try {
					long begin = System.nanoTime();
					for (int i = 0; i < flctc_nr; i++) {
						file = cli.client.create_file(node, repnr, null, null, values);
						System.out.print("Create file: " + file.getFid());
						file.setDigest("MSTOOL_LARGE_SCALE_FILE_TEST");
						file.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE); 
						String path = dm.getPath(file.getLocations().get(0).getDevid(), file.getLocations().get(0).getLocation());
						File nf = new File(path);
						nf.mkdirs();
						System.out.println(", write to location " + path + ", and close it");
						cli.client.close_file(file);
					}
					long end = System.nanoTime();
					System.out.println("--> Create " + flctc_nr + " files in " + (end - begin) / 1000 + " us, CPS is " + flctc_nr * 1000000000.0 / (end - begin));
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-fcrp")) {
				// create a new file by policy
				CreatePolicy cp = new CreatePolicy();
				cp.setOperation(CreateOperation.CREATE_IF_NOT_EXIST_AND_GET_IF_EXIST);
				
				try {
					List<SplitValue> values = new ArrayList<SplitValue>();
					values.add(new SplitValue("rel_time", 1, "1024", 0));
					values.add(new SplitValue("rel_time", 1, "2048", 0));
					values.add(new SplitValue("isp_name", 2, "2", 0));

					file = cli.client.create_file_by_policy(cp, 2, "db1", "wb", values);
					System.out.println("Create file: " + toStringSFile(file));
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-cvt")) {
				// convert date to timestamp
				try {
					Date d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(o.opt);
					System.out.println(d.getTime() / 1000);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				}
			}
			if (o.flag.equals("-fro")) {
				// reopen a file
				boolean ok = false;
				long fid = 0;
				
				try {
					fid = Long.parseLong(o.opt);
					ok = cli.client.reopen_file(fid);
					if (ok) {
						file = cli.client.get_file_by_id(fid);
						System.out.println("Reopen file: " + toStringSFile(file));
					} else {
						System.out.println("Reopen file failed.");
					}
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MetaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-fcr")) {
				// create a new file and return the fid
				try {
					List<SplitValue> values = new ArrayList<SplitValue>();
					//values.add(new SplitValue("COOL_KEY_1", 1, "COOL_KEY_V1", 0));
					//values.add(new SplitValue("COOL_KEY_2", 2, "COOL_KEY_V2", 0));
					file = cli.client.create_file(node, repnr, null, null, values);
					System.out.println("Create file: " + toStringSFile(file));
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-frr")) {
				// read the file object
				try {
					file = cli.client.get_file_by_id(Long.parseLong(o.opt));
					if (file.getLocations() != null && file.getLocationsSize() > 0) {
						for (SFileLocation sfl : file.getLocations()) {
							System.out.println("SFL: node " + sfl.getNode_name() + ", dev " + sfl.getDevid() + ", loc " + sfl.getLocation());
						}
					}
					System.out.println("Read file: " + toStringSFile(file));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MetaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-fcl")) {
				// close the file
				try {
					file = cli.client.get_file_by_id(Long.parseLong(o.opt));
					file.setDigest("MSTOOL Digested!");
					file.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
					cli.client.close_file(file);
					System.out.println("Close file: " + toStringSFile(file));
					DevMap dm = new DevMap();
					String path = dm.getPath(file.getLocations().get(0).getDevid(), file.getLocations().get(0).getLocation());
					System.out.println("File local location is : " + path);
					File nf = new File(path);
					nf.mkdirs();
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MetaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-fcd")) {
				// delete the file
				try {
					file = cli.client.get_file_by_id(Long.parseLong(o.opt));
					cli.client.rm_file_physical(file);
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MetaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (o.flag.equals("-f")) {
				// test file
				try {
					List<SplitValue> values = new ArrayList<SplitValue>();
					//values.add(new SplitValue("COOL_KEY_NAME", 1, "COOL_KEY_VALUE", 0));
					file = cli.client.create_file(node, repnr, null, null, values);
					System.out.println("Create file: " + toStringSFile(file));
					// write something here
					String filepath;
					DevMap dm = new DevMap();
					dm.refreshDevMap();
					DevStat ds;
					do {
						ds = dm.findDev(file.getLocations().get(0).getDevid());
						if (ds == null || ds.mount_point == null) {
							dm.refreshDevMap();
						} else 
							break;
					} while (true);
					filepath = ds.mount_point + "/" + file.getLocations().get(0).getLocation();
					System.out.println("Trying to write to file location: " + filepath);
					File newfile = new File(filepath + "/test_file");
					try {
						newfile.getParentFile().mkdirs();
						newfile.createNewFile();
						FileOutputStream out = new FileOutputStream(filepath + "/test_file");
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
						cli.client.rm_file_physical(file);
						break;
					}
					file.setDigest("DIGESTED!");
					file.getLocations().get(0).setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
					cli.client.close_file(file);
					System.out.println("Closed file: " + toStringSFile(file));
					r = cli.client.get_file_by_id(file.getFid());
					while (r.getStore_status() != MetaStoreConst.MFileStoreStatus.REPLICATED) {
						try {
							Thread.sleep(10000);
							r = cli.client.get_file_by_id(file.getFid());
						} catch (InterruptedException ex) {
							Thread.currentThread().interrupt();
						}
					}
					System.out.println("Read   file: " + toStringSFile(r));
					cli.client.rm_file_physical(r);
					System.out.println("DEL    file: " + r.getFid());
				} catch (FileOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	    }
	}
}