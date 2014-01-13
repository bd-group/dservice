package iie.databak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BusiTypeColumn;
import org.apache.hadoop.hive.metastore.api.BusiTypeDatacenter;
import org.apache.hadoop.hive.metastore.api.Busitype;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreatePolicy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.EquipRoom;
import org.apache.hadoop.hive.metastore.api.FOFailReason;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.FindNodePolicy;
import org.apache.hadoop.hive.metastore.api.GeoLocation;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MSOperation;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SFileRef;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Subpartition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.statfs;
import org.apache.thrift.TException;

import redis.clients.jedis.exceptions.JedisConnectionException;

import com.facebook.fb303.fb_status;


public class ThriftRPC implements org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface{

	private DatabakConf conf;
	private MetadataStorage ms;
	public ThriftRPC(DatabakConf conf)
	{
		this.conf = conf;
		ms = new MetadataStorage(conf);
	}
	@Override
	public long aliveSince() throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCounter(String arg0) throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, Long> getCounters() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCpuProfile(int arg0) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getOption(String arg0) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getOptions() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public fb_status getStatus() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStatusDetails() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getVersion() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reinitialize() throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setOption(String arg0, String arg1) throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean addEquipRoom(EquipRoom arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addGeoLocation(GeoLocation arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addNodeAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addNodeGroup(NodeGroup arg0) throws AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addNodeGroupAssignment(NodeGroup arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addRoleAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addTableNodeDist(String arg0, String arg1, List<String> arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addUserAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean add_datawarehouse_sql(int arg0, String arg1)
			throws InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Index add_index(Index arg0, Table arg1)
			throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node add_node(String arg0, List<String> arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition add_partition(Partition arg0)
			throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int add_partition_files(Partition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add_partition_index(Index arg0, Partition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean add_partition_index_files(Index arg0, Partition arg1,
			List<SFile> arg2, List<Long> arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Partition add_partition_with_environment_context(Partition arg0,
			EnvironmentContext arg1) throws InvalidObjectException,
			AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int add_partitions(List<Partition> arg0)
			throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add_subpartition(String arg0, String arg1,
			List<String> arg2, Subpartition arg3) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int add_subpartition_files(Subpartition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add_subpartition_index(Index arg0, Subpartition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean add_subpartition_index_files(Index arg0, Subpartition arg1,
			List<SFile> arg2, List<Long> arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean alterNodeGroup(NodeGroup arg0)
			throws AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void alter_database(String arg0, Database arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_index(String arg0, String arg1, String arg2, Index arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Node alter_node(String arg0, List<String> arg1, int arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void alter_partition(String arg0, String arg1, Partition arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_partition_with_environment_context(String arg0,
			String arg1, Partition arg2, EnvironmentContext arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_partitions(String arg0, String arg1, List<Partition> arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_table(String arg0, String arg1, Table arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_table_with_environment_context(String arg0, String arg1,
			Table arg2, EnvironmentContext arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void append_busi_type_datacenter(BusiTypeDatacenter arg0)
			throws InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Partition append_partition(String arg0, String arg1,
			List<String> arg2) throws InvalidObjectException,
			AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition append_partition_by_name(String arg0, String arg1,
			String arg2) throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean assiginSchematoDB(String arg0, String arg1,
			List<FieldSchema> arg2, List<FieldSchema> arg3, List<NodeGroup> arg4)
			throws InvalidObjectException, NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean authentication(String arg0, String arg1)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void cancel_delegation_token(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int close_file(SFile arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int createBusitype(Busitype arg0) throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean createSchema(GlobalSchema arg0)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void create_attribution(Database arg0)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_database(Database arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Device create_device(String arg0, int arg1, String arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SFile create_file(String arg0, int arg1, String arg2, String arg3,
			List<SplitValue> arg4) throws FileOperationException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SFile create_file_by_policy(CreatePolicy arg0, int arg1,
			String arg2, String arg3, List<SplitValue> arg4)
			throws FileOperationException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean create_role(Role arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void create_table(Table arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, NoSuchObjectException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_table_by_user(Table arg0, User arg1)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_table_with_environment_context(Table arg0,
			EnvironmentContext arg1) throws AlreadyExistsException,
			InvalidObjectException, MetaException, NoSuchObjectException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean create_type(Type arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean create_user(User arg0) throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean del_device(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int del_node(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean deleteEquipRoom(EquipRoom arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteGeoLocation(GeoLocation arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeGroup(NodeGroup arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeGroupAssignment(NodeGroup arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteRoleAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteSchema(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteTableNodeDist(String arg0, String arg1,
			List<String> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteUserAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete_partition_column_statistics(String arg0, String arg1,
			String arg2, String arg3) throws NoSuchObjectException,
			MetaException, InvalidObjectException, InvalidInputException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete_table_column_statistics(String arg0, String arg1,
			String arg2) throws NoSuchObjectException, MetaException,
			InvalidObjectException, InvalidInputException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void drop_attribution(String arg0, boolean arg1, boolean arg2)
			throws NoSuchObjectException, InvalidOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void drop_database(String arg0, boolean arg1, boolean arg2)
			throws NoSuchObjectException, InvalidOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean drop_index_by_name(String arg0, String arg1, String arg2,
			boolean arg3) throws NoSuchObjectException, MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_partition(String arg0, String arg1, List<String> arg2,
			boolean arg3) throws NoSuchObjectException, MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_partition_by_name(String arg0, String arg1,
			String arg2, boolean arg3) throws NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int drop_partition_files(Partition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean drop_partition_index(Index arg0, Partition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_partition_index_files(Index arg0, Partition arg1,
			List<SFile> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_role(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int drop_subpartition_files(Subpartition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean drop_subpartition_index(Index arg0, Subpartition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_subpartition_index_files(Index arg0, Subpartition arg1,
			List<SFile> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void drop_table(String arg0, String arg1, boolean arg2)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean drop_type(String arg0) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_user(String arg0) throws NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SFile> filterTableFiles(String arg0, String arg1,
			List<SplitValue> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node> find_best_nodes(int arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node> find_best_nodes_in_groups(String arg0, String arg1,
			int arg2, FindNodePolicy arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDMStatus() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GeoLocation getGeoLocationByName(String arg0) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<GeoLocation> getGeoLocationByNames(List<String> arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMP(String arg0, String arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getMaxFid() throws MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getNodeInfo() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GlobalSchema getSchemaByName(String schName)
			throws NoSuchObjectException, MetaException, TException {
		try {
			GlobalSchema gs = (GlobalSchema)ms.readObject(ObjectType.GLOBALSCHEMA, schName);
			return gs;
		} catch (JedisConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public long getSessionId() throws MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<SFile> getTableNodeFiles(String arg0, String arg1, String arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<NodeGroup> getTableNodeGroups(String arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	//把所有本地缓存的database返回
	@Override
	public List<Database> get_all_attributions() throws MetaException,
			TException {
		List<Database> dbs = new ArrayList<Database>();
		dbs.addAll(MetadataStorage.getDatabaseHm().values());
		return dbs;
	}

	@Override
	public List<BusiTypeColumn> get_all_busi_type_cols() throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<BusiTypeDatacenter> get_all_busi_type_datacenters()
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_all_databases() throws MetaException, TException {
		List<String> dbNames = new ArrayList<String>();
		dbNames.addAll(MetadataStorage.getDatabaseHm().keySet());
//		for(String key : MetadataStorage.getDatabaseHm().keySet()){
//			try {
//				Database db = (Database)ms.readObject(ObjectType.DATABASE, key);
//				dbNames.add(db.getName());
//			} catch (JedisConnectionException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//		}
		return dbNames;
	}

	@Override
	public List<Node> get_all_nodes() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_all_tables(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Database get_attribution(String arg0) throws NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String get_config_value(String arg0, String arg1)
			throws ConfigValSecurityException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Database get_database(String dbName) throws NoSuchObjectException,
			MetaException, TException {
		Database db = new Database();
		try {
			db = (Database)ms.readObject(ObjectType.DATABASE, dbName);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return db;
	}

	@Override
	public List<String> get_databases(String pattern) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String get_delegation_token(String arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Device get_device(String arg0) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<FieldSchema> get_fields(String arg0, String arg1)
			throws MetaException, UnknownTableException, UnknownDBException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SFile get_file_by_id(long arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		try {
			SFile f =(SFile) ms.readObject(ObjectType.SFILE, arg0+"");
			if(f == null)
				throw new FileOperationException("File not found by id:"+arg0, FOFailReason.INVALID_FILE);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public SFile get_file_by_name(String arg0, String arg1, String arg2)
			throws FileOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Index get_index_by_name(String dbName, String tableName, String indexName)
			throws MetaException, NoSuchObjectException, TException {
		String key = dbName + "." + tableName + "." + indexName;
		Index ind = MetadataStorage.getIndexHm().get(key);
		return ind;
//		try {
//			ind = (Index)ms.readObject(ObjectType.INDEX, key);
//		} catch (JedisConnectionException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//		return ind;
	}

	@Override
	public List<String> get_index_names(String dbName, String tblName, short arg2)
			throws MetaException, TException {
		List<String> indNames = new ArrayList<String>();
		for(String key : MetadataStorage.getIndexHm().keySet()){
			String[] keys = key.split(".");
			if(dbName.equalsIgnoreCase(keys[0]) && tblName.equalsIgnoreCase(keys[1])){
				indNames.add(keys[2]);
			}
		}
		return indNames;
	}

	@Override
	public List<Index> get_indexes(String dbName, String tblName, short arg2)
			throws NoSuchObjectException, MetaException, TException {
		List<Index> inds = new ArrayList<Index>();
		for(String key : MetadataStorage.getIndexHm().keySet()){
			String[] keys = key.split(".");
			if(dbName.equalsIgnoreCase(keys[0]) && tblName.equalsIgnoreCase(keys[1])){
				inds.add(MetadataStorage.getIndexHm().get(key));
			}
		}
		return inds;
	}

	@Override
	//MetaStoreClient 初始化时会调这个rpc
	public Database get_local_attribution() throws MetaException, TException {
		// TODO Auto-generated method stub
		System.out.println("in get local attibu");
		String dbname = conf.getLocalDbName();
		try {
			Database db = (Database) ms.readObject(ObjectType.DATABASE, dbname);
			return db;
		} catch (JedisConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//如果返回null会抛异常
		//org.apache.thrift.TApplicationException: get_local_attribution failed: unknown result
		return null;
	}

	@Override
	public List<String> get_lucene_index_names(String arg0, String arg1,
			short arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node get_node(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition get_partition(String arg0, String arg1, List<String> arg2)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition get_partition_by_name(String arg0, String arg1, String arg2)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnStatistics get_partition_column_statistics(String arg0,
			String arg1, String arg2, String arg3)
			throws NoSuchObjectException, MetaException, InvalidInputException,
			InvalidObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SFileRef> get_partition_index_files(Index arg0, Partition arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_partition_names(String arg0, String arg1, short arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_partition_names_ps(String arg0, String arg1,
			List<String> arg2, short arg3) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition get_partition_with_auth(String arg0, String arg1,
			List<String> arg2, String arg3, List<String> arg4)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions(String arg0, String arg1, short arg2)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_by_filter(String arg0, String arg1,
			String arg2, short arg3) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_by_names(String arg0, String arg1,
			List<String> arg2) throws MetaException, NoSuchObjectException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_ps(String arg0, String arg1,
			List<String> arg2, short arg3) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_ps_with_auth(String arg0,
			String arg1, List<String> arg2, short arg3, String arg4,
			List<String> arg5) throws NoSuchObjectException, MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_with_auth(String arg0, String arg1,
			short arg2, String arg3, List<String> arg4)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef arg0,
			String arg1, List<String> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_role_names() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<FieldSchema> get_schema(String arg0, String arg1)
			throws MetaException, UnknownTableException, UnknownDBException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SFileRef> get_subpartition_index_files(Index arg0,
			Subpartition arg1) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Subpartition> get_subpartitions(String arg0, String arg1,
			Partition arg2) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table get_table(String arg0, String arg1) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnStatistics get_table_column_statistics(String arg0,
			String arg1, String arg2) throws NoSuchObjectException,
			MetaException, InvalidInputException, InvalidObjectException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_table_names_by_filter(String arg0, String arg1,
			short arg2) throws MetaException, InvalidOperationException,
			UnknownDBException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Table> get_table_objects_by_name(String arg0, List<String> arg1)
			throws MetaException, InvalidOperationException,
			UnknownDBException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_tables(String arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Type get_type(String arg0) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Type> get_type_all(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean grant_privileges(PrivilegeBag arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean grant_role(String arg0, String arg1, PrincipalType arg2,
			String arg3, PrincipalType arg4, boolean arg5)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isPartitionMarkedForEvent(String arg0, String arg1,
			Map<String, String> arg2, PartitionEventType arg3)
			throws MetaException, NoSuchObjectException, UnknownDBException,
			UnknownTableException, UnknownPartitionException,
			InvalidPartitionException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<NodeGroup> listDBNodeGroups(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<EquipRoom> listEquipRoom() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Long> listFilesByDigest(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<GeoLocation> listGeoLocation() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<NodeGroup> listNodeGroupByNames(List<String> arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<NodeGroup> listNodeGroups() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node> listNodes() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Role> listRoles() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<GlobalSchema> listSchemas() throws MetaException, TException {
		List<GlobalSchema> gss = new ArrayList<GlobalSchema>(); 
		for(String gsName : MetadataStorage.getGlobalSchemaHm().keySet()){
			gss.add(MetadataStorage.getGlobalSchemaHm().get(gsName));
		}
		return gss;
	}

	@Override
	public List<Long> listTableFiles(String arg0, String arg1, int arg2,
			int arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<NodeGroup> listTableNodeDists(String arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<User> listUsers() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Device> list_device() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HiveObjectPrivilege> list_privileges(String arg0,
			PrincipalType arg1, HiveObjectRef arg2) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Role> list_roles(String arg0, PrincipalType arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> list_users(Database arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> list_users_names() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void markPartitionForEvent(String arg0, String arg1,
			Map<String, String> arg2, PartitionEventType arg3)
			throws MetaException, NoSuchObjectException, UnknownDBException,
			UnknownTableException, UnknownPartitionException,
			InvalidPartitionException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean migrate2_in(Table arg0, List<Partition> arg1,
			List<Index> arg2, String arg3, String arg4,
			Map<Long, SFileLocation> arg5) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SFileLocation> migrate2_stage1(String arg0, String arg1,
			List<String> arg2, String arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean migrate2_stage2(String arg0, String arg1, List<String> arg2,
			String arg3, String arg4, String arg5) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean migrate_in(Table arg0, Map<Long, SFile> arg1,
			List<Index> arg2, String arg3, String arg4,
			Map<Long, SFileLocation> arg5) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SFileLocation> migrate_stage1(String arg0, String arg1,
			List<Long> arg2, String arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean migrate_stage2(String arg0, String arg1, List<Long> arg2,
			String arg3, String arg4, String arg5, String arg6, String arg7)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyEquipRoom(EquipRoom arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyGeoLocation(GeoLocation arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyNodeGroup(String arg0, NodeGroup arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifySchema(String arg0, GlobalSchema arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Device modify_device(Device arg0, Node arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean modify_user(User arg0) throws NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean offline_filelocation(SFileLocation arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean online_filelocation(SFile arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, String> partition_name_to_spec(String arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> partition_name_to_vals(String arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String pingPong(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void rename_partition(String arg0, String arg1, List<String> arg2,
			Partition arg3) throws InvalidOperationException, MetaException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long renew_delegation_token(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean reopen_file(long arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int restore_file(SFile arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean revoke_privileges(PrivilegeBag arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean revoke_role(String arg0, String arg1, PrincipalType arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int rm_file_logical(SFile arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int rm_file_physical(SFile arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void set_file_repnr(long arg0, int arg1)
			throws FileOperationException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean set_loadstatus_bad(long arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<String> set_ugi(String arg0, List<String> arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Busitype> showBusitypes() throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public statfs statFileSystem(long arg0, long arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean toggle_safemode() throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void truncTableFiles(String arg0, String arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update_attribution(Database arg0) throws NoSuchObjectException,
			InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean update_partition_column_statistics(ColumnStatistics arg0)
			throws NoSuchObjectException, InvalidObjectException,
			MetaException, InvalidInputException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean update_table_column_statistics(ColumnStatistics arg0)
			throws NoSuchObjectException, InvalidObjectException,
			MetaException, InvalidInputException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean user_authority_check(User arg0, Table arg1,
			List<MSOperation> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public int del_fileLocation(SFileLocation arg0)
			throws FileOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}
	

}
