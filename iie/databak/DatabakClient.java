package iie.databak;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.Table;

public class DatabakClient {

	private IDatabakRPC dr;

	public DatabakClient(String server, int port) throws RemoteException, NotBoundException {
		Registry r = LocateRegistry.getRegistry(server, port);
		dr = (IDatabakRPC) r.lookup("DatabakRPC");
	}

	public SFile getSFileById(long id) throws RemoteException {
		return (SFile) dr.getSFileById(id);
	}

	public Table getTable(String dbname, String tablename) throws RemoteException {
		return (Table) dr.getTable(dbname, tablename);
	}

	public Database getDatabase(String dbname) throws RemoteException {
		return null;
	}

	public Node getNode(String nodename) throws RemoteException {
		return null;
	}

	public NodeGroup getNodeGroup(String ngname) throws RemoteException {
		return null;
	}

	public GlobalSchema getSchemaByName(String schemaName) throws RemoteException {

		return (GlobalSchema)dr.getSchemaByName(schemaName);
	}

}
