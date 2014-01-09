package iie.databak;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.Table;

import redis.clients.jedis.exceptions.JedisConnectionException;

public class DatabakRPC extends UnicastRemoteObject implements IDatabakRPC {

	private DatabakConf conf;
	private MetadataStorage ms;
	public DatabakRPC(DatabakConf conf) throws RemoteException
	{
		this.conf = conf;
		ms = new MetadataStorage(conf);
	}
	@Override
	public SFile getSFileById(long id) throws RemoteException {
		// TODO Auto-generated method stub
		try {
			return (SFile) ms.readObject(ObjectType.SFILE, id+"");
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
		return null;
	}
	@Override
	public Table getTable(String dbname, String tablename) throws RemoteException {
		return null;
	}
	@Override
	public Database getDatabase(String dbname) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Node getNode(String nodename) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public NodeGroup getNodeGroup(String ngname) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public GlobalSchema getSchemaByName(String schemaName) throws RemoteException {
		
		try {
			return (GlobalSchema)ms.readObject(ObjectType.GLOBALSCHEMA, schemaName);
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
		return null;
	}
	

}
