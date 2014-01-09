package iie.databak;

import iie.metastore.MetaStoreClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.Table;
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

	private static ConcurrentHashMap<String, Database> databaseHm = new ConcurrentHashMap<String, Database>();
	private static ConcurrentHashMap<String, PrivilegeBag> privilegeBagHm = new ConcurrentHashMap<String, PrivilegeBag>();
	private static ConcurrentHashMap<String, Partition> partitionHm = new ConcurrentHashMap<String, Partition>();
	private static ConcurrentHashMap<String, Node> nodeHm = new ConcurrentHashMap<String, Node>();
	private static ConcurrentHashMap<String, NodeGroup> nodeGroupHm = new ConcurrentHashMap<String, NodeGroup>();
	private static ConcurrentHashMap<String, GlobalSchema> globalSchemaHm = new ConcurrentHashMap<String, GlobalSchema>();
	private static ConcurrentHashMap<String, Table> tableHm = new ConcurrentHashMap<String, Table>();
	private static Map<String, SFile> sFileHm = new LRUMap<String, SFile>(1000);
	private static ConcurrentHashMap<String, Index> indexHm = new ConcurrentHashMap<String, Index>();
	private static ConcurrentHashMap<String, Schema> schemaHm = new ConcurrentHashMap<String, Schema>();
	private static ConcurrentHashMap<String, SFileLocation> sflHm = new ConcurrentHashMap<String, SFileLocation>();
	
	public MetadataStorage(DatabakConf conf) {
		this.conf = conf;
		rf = new RedisFactory(conf);
		
	}

	public int handleMsg(DDLMsg msg) throws JedisConnectionException, IOException, NoSuchObjectException, TException {
		if(msClient == null)
			msClient = new MetaStoreClient(conf.getMshost(), conf.getMsport());
		
		int eventid = (int) msg.getEvent_id();
//		System.out.println("handler msg id:"+eventid);
		switch (eventid) {
			case MSGType.MSG_NEW_DATABESE: 
			case MSGType.MSG_ALTER_DATABESE:
			case MSGType.MSG_ALTER_DATABESE_PARAM:
			{
				String dbName = (String) msg.getMsg_data().get("db_name");
				try{
					Database db = msClient.client.getDatabase(dbName);
					writeObject(ObjectType.DATABASE, dbName, db);
				}catch(NoSuchObjectException e ){
					e.printStackTrace();
				}
				break;
				
			}
			case MSGType.MSG_DROP_DATABESE:
			{
				String dbname = (String) msg.getMsg_data().get("db_name");
				removeObject(ObjectType.DATABASE, dbname);
				break;
			}
			case MSGType.MSG_ALT_TALBE_NAME:
			{
				String dbname = (String) msg.getMsg_data().get("db_name");
				String tablename = (String) msg.getMsg_data().get("table_name");
				String old_tablename = (String) msg.getMsg_data().get("old_table_name");
				Table t = msClient.client.getTable(dbname, tablename);
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
//					msClient.client.getTable(dbname, tablename);
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
				SFile sf = null;
				try{
					sf = msClient.client.get_file_by_id(fid);
				}catch(FileOperationException e)
				{
					//Can not find SFile by FID ...
					System.out.println(e.getMessage());
					if(sf == null)
						break;
				}
				SFileImage sfi = SFileImage.generateSFileImage(sf);
				sFileHm.put(fid+"", sf);
				writeObject(ObjectType.SFILE, fid+"", sfi);
				for(int i = 0;i<sfi.getSflkeys().size();i++)
				{
					writeObject(ObjectType.SFILELOCATION, sfi.getSflkeys().get(i), sf.getLocations().get(i));
				}
				
				break;
			}
			//在删除文件时，会在之前发几个1307,然后才是4002
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
			
			case MSGType.MSG_CREATE_SCHEMA:
			case MSGType.MSG_MODIFY_SCHEMA_DEL_COL:
			case MSGType.MSG_MODIFY_SCHEMA_ADD_COL:
			case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_NAME:
			case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_TYPE:
			case MSGType.MSG_MODIFY_SCHEMA_PARAM:
			{
				String schema_name = (String)msg.getMsg_data().get("schema_name");
				try{
					GlobalSchema s = msClient.client.getSchemaByName(schema_name);
					writeObject(ObjectType.GLOBALSCHEMA, schema_name, s);
				}catch(NoSuchObjectException e){
					e.printStackTrace();
				}
				
				break;
			}
			
			case MSGType.MSG_MODIFY_SCHEMA_NAME:
			{
				String old_schema_name = (String)msg.getMsg_data().get("old_schema_name");
				String schema_name = (String)msg.getMsg_data().get("schema_name");
				GlobalSchema gs = globalSchemaHm.get(old_schema_name);
				if(gs != null)
				{
					removeObject(ObjectType.GLOBALSCHEMA, old_schema_name);
					writeObject(ObjectType.GLOBALSCHEMA, schema_name, gs);
				}
				else{
					try{
						GlobalSchema ngs = msClient.client.getSchemaByName(schema_name);
						writeObject(ObjectType.GLOBALSCHEMA, schema_name, ngs);
					}
					catch(NoSuchObjectException e)
					{
						e.printStackTrace();
					}
				}
				break;
			}
			
			case MSGType.MSG_DEL_SCHEMA:
			{
				String schema_name = (String)msg.getMsg_data().get("schema_name");
				removeObject(ObjectType.GLOBALSCHEMA, schema_name);
				break;
			}
			
			//what can I do...
		    case MSGType.MSG_GRANT_GLOBAL:
		    case MSGType.MSG_GRANT_DB:
		    case MSGType.MSG_GRANT_TABLE:
		    case MSGType.MSG_GRANT_SCHEMA:
		    case MSGType.MSG_GRANT_PARTITION:
		    case MSGType.MSG_GRANT_PARTITION_COLUMN:
		    case MSGType.MSG_GRANT_TABLE_COLUMN:
	
		    case MSGType.MSG_REVOKE_GLOBAL:
		    case MSGType.MSG_REVOKE_DB:
		    case MSGType.MSG_REVOKE_TABLE:
		    case MSGType.MSG_REVOKE_PARTITION:
		    case MSGType.MSG_REVOKE_SCHEMA:
		    case MSGType.MSG_REVOKE_PARTITION_COLUMN:
		    case MSGType.MSG_REVOKE_TABLE_COLUMN:
		    {
//		    	msClient.client.
		    	break;
		    }
			
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
		//对于sfile，函数参数是sfileimage
//		if(key.equals(ObjectType.SFILE))
//			sFileHm.put(field, (SFile)o);
		if(key.equals(ObjectType.SFILELOCATION))
			sflHm.put(field, (SFileLocation)o);
		if(key.equals(ObjectType.INDEX))
			indexHm.put(field, (Index)o);
//		if(key.equals(ObjectType.NODE))
//			nodeHm.put(field, (Node)o);
//		if(key.equals(ObjectType.NODEGROUP))
//			nodeGroupHm.put(field, (NodeGroup)o);
//		if(key.equals(ObjectType.GLOBALSCHEMA))
			globalSchemaHm.put(field, (GlobalSchema)o);
		if(key.equals(ObjectType.PRIVILEGE))
			privilegeBagHm.put(field, (PrivilegeBag)o);
		if(key.equals(ObjectType.PARTITION))
			partitionHm.put(field, (Partition)o);
	}

	public Object readObject(String key, String field)throws JedisConnectionException, IOException,ClassNotFoundException {
		Object o = null;
		if(key.equals(ObjectType.DATABASE))
			o = databaseHm.get(field);
		if(key.equals(ObjectType.TABLE))
			o = tableHm.get(field);
		if(key.equals(ObjectType.SFILE))
			o = sFileHm.get(field);
		if(key.equals(ObjectType.SFILELOCATION))
			o = sflHm.get(field);
		if(key.equals(ObjectType.INDEX))
			o = indexHm.get(field);
		if(key.equals(ObjectType.NODE))
			o = nodeHm.get(field);
		if(key.equals(ObjectType.NODEGROUP))
			o = nodeGroupHm.get(field);
		if(key.equals(ObjectType.GLOBALSCHEMA))
			o = globalSchemaHm.get(field);
		if(key.equals(ObjectType.PRIVILEGE))
			o = privilegeBagHm.get(field);
		if(key.equals(ObjectType.PARTITION))
			o = partitionHm.get(field);
		
		if(o != null)
		{
			System.out.println("in function readObject: read "+key+" from cache.");
			return o;
		}
		//SFile 要特殊处理
		reconnectJedis();
		if(key.equals(ObjectType.SFILE))
		{
			byte[] buf = jedis.hget(key.getBytes(), field.getBytes());
			if(buf == null)
				return null;
			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			ObjectInputStream ois = new ObjectInputStream(bais);
			SFileImage sfi = (SFileImage)ois.readObject();
			List<SFileLocation> locations = new ArrayList<SFileLocation>();
			for(int i = 0;i<sfi.getSflkeys().size();i++)
			{
				locations.add((SFileLocation)readObject(ObjectType.SFILELOCATION, sfi.getSflkeys().get(i)));
			}
			return new SFile(sfi.getFid(),sfi.getDbName(),sfi.getTableName(),sfi.getStore_status(),sfi.getRep_nr(),
					sfi.getDigest(),sfi.getRecord_nr(),sfi.getAll_record_nr(),locations,sfi.getLength(),
					sfi.getRef_files(),sfi.getValues(),sfi.getLoad_status());
		}
		
		byte[] buf = jedis.hget(key.getBytes(), field.getBytes());
		if(buf == null)
			return null;
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		ObjectInputStream ois = new ObjectInputStream(bais);
		o = ois.readObject();
		
		System.out.println("in function readObject: read "+key+" from redis");
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
		if(key.equals(ObjectType.GLOBALSCHEMA))
			globalSchemaHm.remove(field);
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
