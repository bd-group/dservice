package iie.metastore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

public class MetaStoreClient {
	public IMetaStoreClient client;
	
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
		HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
			public HiveMetaHook getHook(
					org.apache.hadoop.hive.metastore.api.Table tbl)
					throws MetaException {

				return null;
			}
		};
		return RetryingMetaStoreClient.getProxy("thrift://localhost:9083", 5, 1, hookLoader, HiveMetaStoreClient.class.getName());
	}
	
	public void init() {
		try {
			client = createMetaStoreClient();
		} catch (MetaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void stop() {
		if (client != null) {
			client.close();
		}
	}
	
	public static String toStringSFile(SFile file) {
		if (file == null) {
			return "null";
		}
		
		String r = "<";
		r += "fid:" + file.getFid() + ", ";
		r += "placement:" + file.getPlacement() + ", ";
		r += "storestatus: " + file.getStore_status() + ", ";
		r += "repnr: " + file.getRep_nr() + ", ";
		r += "digest: " + file.getDigest() + ", ";
		r += "record_nr: " + file.getRecord_nr() + ", ";
		r += "allrecord_nr: " + file.getAll_record_nr() + ", [\n";
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
	
	public static void main(String[] args) {
		MetaStoreClient cli = new MetaStoreClient();
		cli.init();
		String node = null;
		List<String> ipl = new ArrayList<String>();
		ipl.add("127.0.0.1");
		long table_id = 1;
		int repnr = 3;
		SFile file = null, r = null;
		
		try {
			node = InetAddress.getLocalHost().getHostName();
			Node n = cli.client.add_node(node, ipl);
		} catch (MetaException me) {
			me.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			file = cli.client.create_file(node, repnr, table_id);
			System.out.println("Create file: " + toStringSFile(file));
			file.setDigest("DIGESTED!");
			cli.client.close_file(file);
			System.out.println("Closed file: " + toStringSFile(file));
			try {
				Thread.sleep(10000);
			} catch(InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			r = cli.client.get_file_by_id(file.getFid());
			System.out.println("Read   file: " + toStringSFile(r));
		} catch (FileOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}