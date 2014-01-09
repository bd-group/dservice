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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
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

	private ConcurrentHashMap<String, Database> databaseHm = new ConcurrentHashMap<String, Database>();
	private ConcurrentHashMap<String, Table> tableHm = new ConcurrentHashMap<String, Table>();
	private ConcurrentHashMap<String, FieldSchema> fieSchHm = new ConcurrentHashMap<String, FieldSchema>();
	private Map<String, SFileImage> sFileHm = new LRUMap<String, SFileImage>(1000);
	private ConcurrentHashMap<String, SFileLocation> sflHm = new ConcurrentHashMap<String, SFileLocation>();
	private ConcurrentHashMap<String, Node> nodeHm = new ConcurrentHashMap<String, Node>();
	private ConcurrentHashMap<String, NodeGroupImage> nodeGroupHm = new ConcurrentHashMap<String, NodeGroupImage>();
	private ConcurrentHashMap<String, Index> indexHm = new ConcurrentHashMap<String, Index>();
	private ConcurrentHashMap<String, GlobalSchema> globalSchemaHm = new ConcurrentHashMap<String, GlobalSchema>();
	//private ConcurrentHashMap<String, PrivilegeBag> privilegeBagHm = new ConcurrentHashMap<String, PrivilegeBag>();
	//private ConcurrentHashMap<String, Partition> partitionHm = new ConcurrentHashMap<String, Partition>();
	
	
	
	public MetadataStorage(DatabakConf conf) {
		this.conf = conf;
		rf = new RedisFactory(conf);
		
	}

	public int handleMsg(DDLMsg msg) {
		try {
			if(msClient == null)
				msClient = new MetaStoreClient(conf.getMshost(), conf.getMsport());
		} catch (MetaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int handleMsg(DDLMsg msg) throws Exception {
		int eventId = (int) msg.getEvent_id();
//		System.out.println("handler msg id:"+eventid);
		try {
			switch (eventId) {
				//database msg operation.
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
					String dbName = (String) msg.getMsg_data().get("db_name");
					if(databaseHm.remove(dbName) != null)
						removeObject(ObjectType.DATABASE, dbName);
					break;
				}
				//table msg operation.
				case MSGType.MSG_NEW_TALBE:
				case MSGType.MSG_ALT_TALBE_DISTRIBUTE:
				case MSGType.MSG_ALT_TABLE_SPLITKEYS:
				case MSGType.MSG_ALT_TALBE_DEL_COL:
				case MSGType.MSG_ALT_TALBE_ADD_COL:
				case MSGType.MSG_ALT_TALBE_ALT_COL_NAME:
				case MSGType.MSG_ALT_TALBE_ALT_COL_TYPE:
				{
					String dbName = (String) msg.getMsg_data().get("db_name");
					String tableName = (String) msg.getMsg_data().get("table_name");
					String key = dbName + "." + tableName;
					Table tbl = msClient.client.getTable(dbName, tableName);
					TableImage ti = TableImage.generateTableImage(tbl);
					writeObject(ObjectType.TABLE, key, ti);
					for(int i = 0; i<ti.getNgKeys().size();i++){
						if(!nodeGroupHm.containsKey(ti.getNgKeys().get(i))){
							List<String> ngNames = new ArrayList<String>();
							ngNames.add(ti.getNgKeys().get(i));
							List<NodeGroup> ngs = msClient.client.listNodeGroups(ngNames);
							NodeGroup ng = ngs.get(0);
							NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ng);
							writeObject(ObjectType.NODEGROUP, ng.getNode_group_name(), ngi);
							for(int j = 0; j<ngi.getNodeKeys().size();j++){
								if(!nodeHm.containsKey(ngi.getNodeKeys().get(j))){
									Node node = msClient.client.get_node(ngi.getNodeKeys().get(j));
									writeObject(ObjectType.NODE, ngi.getNodeKeys().get(j), node);
								}
							}
						}
					}
					/*for(int i = 0;i<ti.getFsKeys().size();i++)
					{
						writeObject(ObjectType.FIELDSCHEMA, ti.getFsKeys().get(i), tbl.getFileSplitKeys().get(i));
					}
					StorageDescriptorImage sdci = StorageDescriptorImage.generateStorageDescriptor
							(tbl.getDbName(), tbl.getTableName(), tbl.getSd());
					writeObject(ObjectType.STORAGEDESCRIPTOR, ti.getSdKey(), sdci);
					for(int i = 0;i<sdci.getColsKeys().size();i++)
					{
						writeObject(ObjectType.FIELDSCHEMA, sdci.getColsKeys().get(i), tbl.getSd().getCols().get(i));
					}
					for(int i = 0;i<tbl.getNodeGroups().size();i++){
						
						NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ti.getNgKeys().get(i), tbl.getNodeGroups().get(i));
						writeObject(ObjectType.NODEGROUP, ti.getNgKeys().get(i), tbl.getNodeGroups().get(i));
						for(int j = 0;j<ngi.getNodeKeys().size();j++)
						{
							
						}
					} */
					break;
				}
				/*{
					String dbName = (String) msg.getMsg_data().get("db_name");
					String tableName = (String) msg.getMsg_data().get("table_name");
					String ngName = (String) msg.getMsg_data().get("nodeGroup_name");
					String ngKey = dbName + "." + tableName + ".ng." + ngName;
					Table tbl = msClient.client.getTable(dbName, tableName);
					List<NodeGroup> ngs = tbl.getNodeGroups();
					for(NodeGroup ng : ngs){
						if(ng.getNode_group_name().equalsIgnoreCase(ngName)){
							NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ngKey, ng);
							writeObject(ObjectType.NODEGROUP, ngKey, ngi);
							for(int j = 0;j<ngi.getNodeKeys().size();j++)
							{
								writeObject(ObjectType.NODE, ngi.getNodeKeys().get(j), tbl.getNodeGroups().get(j));
							}
						}
					}
				}
				
				{
					String dbName = (String) msg.getMsg_data().get("db_name");
					String tableName = (String) msg.getMsg_data().get("table_name");
					long version = (Long) msg.getMsg_data().get("version");
					Table tbl = msClient.client.getTable(dbName, tableName);
					List<FieldSchema> fss = tbl.getFileSplitKeys();
					for(FieldSchema fs : fss){
						if(fs.getVersion() == version){
							String fskey = dbName + "." + tableName + ".fs." + fs.getName();
							writeObject(ObjectType.FIELDSCHEMA, fskey, fs);
						}
					}
					//TabletableHm.get( )
				}*/
				
				
				case MSGType.MSG_ALT_TALBE_NAME:
				{
					String dbName = (String) msg.getMsg_data().get("db_name");
					String tableName = (String) msg.getMsg_data().get("table_name");
					String oldTableName = (String) msg.getMsg_data().get("old_table_name");
					String oldKey = dbName + "." + oldTableName;
					String newKey = dbName + "." + tableName;
					if(tableHm.remove(oldKey) != null){
						removeObject(ObjectType.TABLE, oldKey);
					}
					Table tbl = msClient.client.getTable(dbName, tableName);
					TableImage ti = TableImage.generateTableImage(tbl);
					writeObject(ObjectType.TABLE, newKey, ti);
					for(int i = 0; i<ti.getNgKeys().size();i++){
						if(!nodeGroupHm.containsKey(ti.getNgKeys().get(i))){
							List<String> ngNames = new ArrayList<String>();
							ngNames.add(ti.getNgKeys().get(i));
							List<NodeGroup> ngs = msClient.client.listNodeGroups(ngNames);
							NodeGroup ng = ngs.get(0);
							NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ng);
							writeObject(ObjectType.NODEGROUP, ng.getNode_group_name(), ngi);
							for(int j = 0; j<ngi.getNodeKeys().size();j++){
								if(!nodeHm.containsKey(ngi.getNodeKeys().get(j))){
									Node node = msClient.client.get_node(ngi.getNodeKeys().get(j));
									writeObject(ObjectType.NODE, ngi.getNodeKeys().get(j), node);
								}
							}
						}
					}
					break;
				}
				
				case MSGType.MSG_DROP_TABLE:
				{
					String dbName = (String) msg.getMsg_data().get("db_name");
					String tableName = (String) msg.getMsg_data().get("table_name");
					String key = dbName + "." + tableName;
					if(tableHm.remove(key) != null){
						removeObject(ObjectType.TABLE, key);
					}
					break;
				}
				//file msg operation.
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
					SFileImage sf = sFileHm.get(fid+"");
					if(sf != null)
					{
						for(String sflKey : sf.getSflkeys())
						{
							//String key = sfl.getLocation()+"_"+sfl.getDevid();
							removeObject(ObjectType.SFILELOCATION, sflKey);
						}
						removeObject(ObjectType.SFILE, fid+"");
					}
					break;
				}
				
				case MSGType.MSG_NEW_INDEX:
//				case MSGType.MSG_ALT_INDEX:
//				case MSGType.MSG_ALT_INDEX_PARAM:
				{
					String dbName = (String)msg.getMsg_data().get("db_name");
					String tblName = (String)msg.getMsg_data().get("table_name");
					String indexName = (String)msg.getMsg_data().get("index_name");
					Index ind = msClient.client.getIndex(dbName, tblName, indexName);
					String key = dbName + "." + tblName + "." + indexName;
					writeObject(ObjectType.INDEX, key, ind);
					break;
				}
				case MSGType.MSG_DEL_INDEX:
				{
					String dbName = (String)msg.getMsg_data().get("db_name");
					String tblName = (String)msg.getMsg_data().get("table_name");
					String indexName = (String)msg.getMsg_data().get("index_name");
					Index ind = msClient.client.getIndex(dbName, tblName, indexName);
					String key = dbName + "." + tblName + "." + indexName;
					if(indexHm.remove(key) != null)
						removeObject(ObjectType.INDEX, key);
					break;
				}
				
				case MSGType.MSG_NEW_NODE:
				case MSGType.MSG_FAIL_NODE:
				case MSGType.MSG_BACK_NODE:
				{
					String nodeName = (String)msg.getMsg_data().get("node_name");
					Node node = msClient.client.get_node(nodeName);
					writeObject(ObjectType.NODE, nodeName, node);
					break;
				}
				
				case MSGType.MSG_DEL_NODE:
				{
					String nodeName = (String)msg.getMsg_data().get("node_name");
					for(String ngiKey : nodeGroupHm.keySet()){
						NodeGroupImage ngi = nodeGroupHm.get(ngiKey);
						if(ngi.getNodeKeys().contains(nodeName)){
							throw new Exception("the node"+ nodeName + "has be used in nodegroup.");
						}else{
							removeObject(ObjectType.NODE, nodeName);
						}
					}
					break;
				}
				case MSGType.MSG_NEW_NODEGROUP:
				{
					String nodeGroupName = (String)msg.getMsg_data().get("nodegroup_name");
					List<String> ngNames = new ArrayList<String>();
					ngNames.add(nodeGroupName);
					List<NodeGroup> ngs = msClient.client.listNodeGroups(ngNames);
					NodeGroup ng = ngs.get(0);
					NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ng);
					writeObject(ObjectType.NODEGROUP, ng.getNode_group_name(), ngi);
					for(int i = 0; i<ngi.getNodeKeys().size();i++){
						if(!nodeHm.containsKey(ngi.getNodeKeys().get(i))){
							Node node = msClient.client.get_node(ngi.getNodeKeys().get(i));
							writeObject(ObjectType.NODE, ngi.getNodeKeys().get(i), node);
						}
					}
				}
				//case MSGType.MSG_ALTER_NODEGROUP:
				case MSGType.MSG_DEL_NODEGROUP:{
					String nodeGroupName = (String)msg.getMsg_data().get("nodegroup_name");
					removeObject(ObjectType.NODEGROUP, nodeGroupName);
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
		if(key.equals(ObjectType.FIELDSCHEMA))
			fieSchHm.put(field, (FieldSchema)o);
		if(key.equals(ObjectType.SFILE))
			sFileHm.put(field, (SFileImage)o);
		if(key.equals(ObjectType.SFILELOCATION))
			sflHm.put(field, (SFileLocation)o);
		if(key.equals(ObjectType.INDEX))
			indexHm.put(field, (Index)o);
		if(key.equals(ObjectType.NODE))
			nodeHm.put(field, (Node)o);
		if(key.equals(ObjectType.NODEGROUP))
			nodeGroupHm.put(field, (NodeGroupImage)o);
		if(key.equals(ObjectType.SCHEMA))
			schemaHm.put(field, (Schema)o);
//		if(key.equals(ObjectType.PRIVILEGE))
//			privilegeBagHm.put(field, (PrivilegeBag)o);
//		if(key.equals(ObjectType.PARTITION))
//			partitionHm.put(field, (Partition)o);
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
		if(key.equals(ObjectType.SCHEMA))
			o = schemaHm.get(field);
		if(key.equals(ObjectType.PRIVILEGE))
			o = privilegeBagHm.get(field);
		if(key.equals(ObjectType.PARTITION))
			o = partitionHm.get(field);
		
		if(o != null)
			return o;
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
//		if(key.equals(ObjectType.PRIVILEGE))
//			privilegeBagHm.remove(field);
//		if(key.equals(ObjectType.PARTITION))
//			partitionHm.remove(field);
		
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
