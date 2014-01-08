package iie.databak;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

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
	public Table getTable(String dbname, String tablename)
			throws RemoteException {
		return null;
	}
	

}
