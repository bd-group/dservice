package iie.databak;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.apache.hadoop.hive.metastore.api.SFile;

public class DatabakClient {

	private IDatabakRPC dr;
	public DatabakClient(String server, int port) throws RemoteException, NotBoundException
	{
		Registry r = LocateRegistry.getRegistry(server, port);
		dr = (IDatabakRPC) r.lookup("DatabakRPC");
	}
	
	public SFile getSFileById(long id) throws RemoteException
	{
		return (SFile)dr.getSFileById(id);
	}
}
