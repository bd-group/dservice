package iie.databak;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.Table;

public interface IDatabakRPC extends Remote {
	Database getDatabase(String dbname) throws RemoteException;
	SFile getSFileById(long id) throws RemoteException;
	Table getTable(String dbname, String tablename) throws RemoteException;
	Node getNode(String nodename) throws RemoteException;
	NodeGroup getNodeGroup(String ngname) throws RemoteException;
	GlobalSchema getSchemaByName(String schemaName) throws RemoteException;
	
}
