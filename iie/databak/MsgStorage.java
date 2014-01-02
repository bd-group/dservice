package iie.databak;

import iie.metastore.MetaStoreClient;
import iie.databak.RedisFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.msg.MSGType;
import org.apache.thrift.TException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class MsgStorage {

	public MetaStoreClient msClient;
	private Jedis jedis;
	private RedisFactory rf;
	private DatabakConf conf;

	private ConcurrentHashMap<String, Database> databaseHm = new ConcurrentHashMap<String, Database>();
	private ConcurrentHashMap<String, PrivilegeBag> privilegeBagHm = new ConcurrentHashMap<String, PrivilegeBag>();
	private ConcurrentHashMap<String, Partition> partitionHm = new ConcurrentHashMap<String, Partition>();
	private ConcurrentHashMap<String, Node> nodeHm = new ConcurrentHashMap<String, Node>();
	private ConcurrentHashMap<String, NodeGroup> nodeGroupHm = new ConcurrentHashMap<String, NodeGroup>();
	private ConcurrentHashMap<String, GlobalSchema> globalSchemaHm = new ConcurrentHashMap<String, GlobalSchema>();
	private ConcurrentHashMap<String, Table> tableHm = new ConcurrentHashMap<String, Table>();
	private ConcurrentHashMap<String, SFile> sFileHm = new ConcurrentHashMap<String, SFile>();
	private ConcurrentHashMap<String, Index> indexHm = new ConcurrentHashMap<String, Index>();
	private ConcurrentHashMap<String, Schema> schemaHm = new ConcurrentHashMap<String, Schema>();

	public MsgStorage(DatabakConf conf) throws MetaException {
		this.conf = conf;
		rf = new RedisFactory(conf);
		msClient = new MetaStoreClient(conf.getMshost(), conf.getMsport());
	}

	public int handleMsg(DDLMsg msg) {
		int eventid = (int) msg.getEvent_id();
		switch (eventid) {
		case MSGType.MSG_NEW_DATABESE: 
			String dbName = (String) msg.getMsg_data().get("dbname");
			try {
				Database db = msClient.client.getDatabase(dbName);
				databaseHm.put(dbName, db);
			} catch (NoSuchObjectException e) {
				e.printStackTrace();
				return 0;
			} catch (MetaException e) {
				e.printStackTrace();
				return 0;
			} catch (TException e) {
				e.printStackTrace();
				return 0;
			}
			return 1;
		
		}
		return 1;
	}

	private void writeObject(String key, String field, Object o)
			throws JedisConnectionException, IOException {
		reconnectJedis();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(o);
		jedis.hset(key.getBytes(), field.getBytes(), baos.toByteArray());
	}

	private Object readObject(String key, String field)
			throws JedisConnectionException, IOException,
			ClassNotFoundException {
		reconnectJedis();

		ByteArrayInputStream bais = new ByteArrayInputStream(jedis.hget(
				key.getBytes(), field.getBytes()));
		ObjectInputStream ois = new ObjectInputStream(bais);
		Object o = ois.readObject();
		return o;
	}

	public void reconnectJedis() throws JedisConnectionException {
		if (jedis == null) {
			jedis = rf.getDefaultInstance();
		}
		if (jedis == null)
			throw new JedisConnectionException(
					"Connection to redis Server failed.");
	}
}
