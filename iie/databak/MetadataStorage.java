package iie.databak;

import iie.metastore.MetaStoreClient;
import iie.databak.RedisFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
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

public class MetadataStorage {

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
	private Map<String, SFile> sFileHm = new LRUMap<String, SFile>(1000);
	private ConcurrentHashMap<String, Index> indexHm = new ConcurrentHashMap<String, Index>();
	private ConcurrentHashMap<String, Schema> schemaHm = new ConcurrentHashMap<String, Schema>();
	private ConcurrentHashMap<String, SFileLocation> sflHm = new ConcurrentHashMap<String, SFileLocation>();
	
	public MetadataStorage(DatabakConf conf) {
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
				case MSGType.MSG_ALTER_DATABESE:
				case MSGType.MSG_ALTER_DATABESE_PARAM:
				{
					String dbName = (String) msg.getMsg_data().get("db_name");
					Database db = msClient.client.getDatabase(dbName);
					writeObject(ObjectType.DATABASE, dbName, db);
					break;
					
				}
				case MSGType.MSG_DROP_DATABESE:
				{
					String dbname = (String) msg.getMsg_data().get("db_name");
					if(databaseHm.remove(dbname) != null)
						removeObject(ObjectType.DATABASE, dbname);
					break;
				}
				case MSGType.MSG_ALT_TALBE_NAME:
				{
					String dbname = (String) msg.getMsg_data().get("db_name");
					String tablename = (String) msg.getMsg_data().get("table_name");
					String old_tablename = (String) msg.getMsg_data().get("old_table_name");
					//rm old, put new
					break;
				}
				case MSGType.MSG_NEW_TALBE:
				case MSGType.MSG_ALT_TALBE_DISTRIBUTE:
				case MSGType.MSG_ALT_TALBE_PARTITIONING:
				case MSGType.MSG_ALT_TABLE_SPLITKEYS:
				case MSGType.MSG_ALT_TALBE_DEL_COL:
				case MSGType.MSG_ALT_TALBE_ADD_COL:
				case MSGType.MSG_ALT_TALBE_ALT_COL_NAME:
				case MSGType.MSG_ALT_TALBE_ALT_COL_TYPE:
				case MSGType.MSG_ALT_TABLE_PARAM:
				{
					String dbname = (String) msg.getMsg_data().get("db_name");
					String tablename = (String) msg.getMsg_data().get("table_name");
					//tablename,dbname  or dbname ,tablename  
//					msClient.client.getTable(arg0, arg1)
					break;
				}
				case MSGType.MSG_DROP_TABLE:
				{
					
					break;
				}
				
				case MSGType.MSG_REP_FILE_CHANGE:
				case MSGType.MSG_STA_FILE_CHANGE:
				case MSGType.MSG_REP_FILE_ONOFF:
				case MSGType.MSG_CREATE_FILE:
				{
					long fid = Long.parseLong(msg.getMsg_data().get("f_id").toString());
					SFile sf = msClient.client.get_file_by_id(fid);
					SFileImage sfi = SFileImage.generateSFileImage(sf);
					writeObject(ObjectType.SFILE, fid+"", sfi);
					for(int i = 0;i<sfi.getSflkeys().size();i++)
					{
						writeObject(ObjectType.SFILELOCATION, sfi.getSflkeys().get(i), sf.getLocations().get(i));
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
						}
						removeObject(ObjectType.SFILE, fid+"");
					}
					break;
				}
				
				case MSGType.MSG_NEW_INDEX:
				case MSGType.MSG_ALT_INDEX:
				case MSGType.MSG_ALT_INDEX_PARAM:
				{
//					String 
//					msClient.client.getIndex(arg0, arg1, arg2)
					break;
				}
				case MSGType.MSG_DEL_INDEX:
				{
					break;
				}
				
				case MSGType.MSG_NEW_NODE:
				case MSGType.MSG_FAIL_NODE:
				case MSGType.MSG_BACK_NODE:
				{
					String nodename = (String)msg.getMsg_data().get("node_name");
					Node node = msClient.client.get_node(nodename);
					writeObject(ObjectType.NODE, nodename, node);
					break;
				}
				
				case MSGType.MSG_DEL_NODE:
				{
					String nodename = (String)msg.getMsg_data().get("node_name");
					removeObject(ObjectType.NODE, nodename);
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
		
		if(key.equals(ObjectType.DATABASE))
			databaseHm.put(field, (Database)o);
		if(key.equals(ObjectType.TABLE))
			tableHm.put(field, (Table)o);
		if(key.equals(ObjectType.SFILE))
			sFileHm.put(field, (SFile)o);
		if(key.equals(ObjectType.SFILELOCATION))
			sflHm.put(field, (SFileLocation)o);
		if(key.equals(ObjectType.INDEX))
			indexHm.put(field, (Index)o);
		if(key.equals(ObjectType.NODE))
			nodeHm.put(field, (Node)o);
		if(key.equals(ObjectType.NODEGROUP))
			nodeGroupHm.put(field, (NodeGroup)o);
		if(key.equals(ObjectType.SCHEMA))
			schemaHm.put(field, (Schema)o);
		if(key.equals(ObjectType.PRIVILEGE))
			privilegeBagHm.put(field, (PrivilegeBag)o);
		if(key.equals(ObjectType.PARTITION))
			partitionHm.put(field, (Partition)o);
	}

	//是不是应该实现成先从缓存中读
	private Object readObject(String key, String field)throws JedisConnectionException, IOException,ClassNotFoundException {
		reconnectJedis();
		ByteArrayInputStream bais = new ByteArrayInputStream(jedis.hget(key.getBytes(), field.getBytes()));
		ObjectInputStream ois = new ObjectInputStream(bais);
		Object o = ois.readObject();
		return o;
	}

	private void removeObject(String key, String field)
	{
		if(key.equals(ObjectType.DATABASE))
			databaseHm.remove(field);
		if(key.equals(ObjectType.TABLE))
			tableHm.remove(field);
		if(key.equals(ObjectType.SFILE))
			sFileHm.remove(field);
		if(key.equals(ObjectType.SFILELOCATION))
			sflHm.remove(field);
		if(key.equals(ObjectType.INDEX))
			indexHm.remove(field);
		if(key.equals(ObjectType.NODE))
			nodeHm.remove(field);
		if(key.equals(ObjectType.NODEGROUP))
			nodeGroupHm.remove(field);
		if(key.equals(ObjectType.SCHEMA))
			schemaHm.remove(field);
		if(key.equals(ObjectType.PRIVILEGE))
			privilegeBagHm.remove(field);
		if(key.equals(ObjectType.PARTITION))
			partitionHm.remove(field);
		
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
