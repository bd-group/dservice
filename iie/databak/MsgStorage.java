package iie.databak;

import iie.metastore.MetaStoreClient;

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

public class MsgStorage {

	public MetaStoreClient msClient;
	
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
	
	
	public MsgStorage() throws MetaException {
		msClient = new MetaStoreClient();
	}

	public MsgStorage(MetaStoreClient msClient) throws MetaException {
		super();
		this.msClient = msClient;
	}
	
	public int createOrAlterDb (DDLMsg msg){
		int msgId = (int)msg.getEvent_id();
		switch(msgId){
			case MSGType.MSG_NEW_DATABESE:
				{
					String dbName = (String)msg.getMsg_data().get("dbname");
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
			}
			return 1;
	}
}
