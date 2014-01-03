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
import org.apache.hadoop.hive.metastore.api.SFileLocation;
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
	private ConcurrentHashMap<String, SFileLocation> sflHm = new ConcurrentHashMap<String, SFileLocation>();
	
	public MsgStorage(DatabakConf conf) {
		this.conf = conf;
		rf = new RedisFactory(conf);
		try {
			msClient = new MetaStoreClient(conf.getMshost(), conf.getMsport());
		} catch (MetaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int handleMsg(DDLMsg msg) {
		int eventid = (int) msg.getEvent_id();
//		System.out.println("handler msg id:"+eventid);
		try {
			switch (eventid) {
			
				case MSGType.MSG_NEW_DATABESE: 
	//				String dbName = (String) msg.getMsg_data().get("dbname");
	//				Database db = msClient.client.getDatabase(dbName);
	//				databaseHm.put(dbName, db);
					break;
				
				case MSGType.MSG_REP_FILE_CHANGE:
				case MSGType.MSG_STA_FILE_CHANGE:
				case MSGType.MSG_REP_FILE_ONOFF:
				case MSGType.MSG_CREATE_FILE:
				{
					long fid = Long.parseLong(msg.getMsg_data().get("f_id").toString());
					SFile sf = msClient.client.get_file_by_id(fid);
					SFileImage sfi = SFileImage.generateSFileImage(sf);
					writeObject(ObjectType.SFILE, fid+"", sfi);
					sFileHm.put(fid+"", sf);
					for(int i = 0;i<sfi.getSflkeys().size();i++)
					{
						writeObject(ObjectType.SFILELOCATION, sfi.getSflkeys().get(i), sf.getLocations().get(i));
						sflHm.put(sfi.getSflkeys().get(i), sf.getLocations().get(i));
					}
	//				System.out.println(f.getLocations().get(0).getDevid());
					
					break;
				}
				case MSGType.MSG_DEL_FILE:
				{
					long fid = Long.parseLong(msg.getMsg_data().get("f_id").toString());
					SFile sf = sFileHm.get(fid+"");
					if(sf != null)
					{
						for(SFileLocation sfl : sf.getLocations())
						{
							String key = sfl.getLocation()+"_"+sfl.getDevid();
							removeObject(ObjectType.SFILELOCATION, key);
							sflHm.remove(key);
						}
						removeObject(ObjectType.SFILE, fid+"");
						sFileHm.remove(fid+"");
					}
					break;
				}
			}
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
			return 0;
		} catch (MetaException e) {
			e.printStackTrace();
			return 0;
		} catch (TException e) {
			e.printStackTrace();
			return 0;
		} catch (JedisConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 1;
	}

	private void writeObject(String key, String field, Object o)throws JedisConnectionException, IOException {
		reconnectJedis();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(o);
		jedis.hset(key.getBytes(), field.getBytes(), baos.toByteArray());
	}

	private Object readObject(String key, String field)throws JedisConnectionException, IOException,ClassNotFoundException {
		reconnectJedis();
		ByteArrayInputStream bais = new ByteArrayInputStream(jedis.hget(key.getBytes(), field.getBytes()));
		ObjectInputStream ois = new ObjectInputStream(bais);
		Object o = ois.readObject();
		return o;
	}

	private void removeObject(String key, String field)
	{
		reconnectJedis();
		jedis.hdel(key, field);
	}
	private void reconnectJedis() throws JedisConnectionException {
		if (jedis == null) {
			jedis = rf.getDefaultInstance();
		}
		if (jedis == null)
			throw new JedisConnectionException("Connection to redis Server failed.");
	}
}
