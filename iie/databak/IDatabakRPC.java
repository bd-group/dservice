package iie.databak;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.apache.hadoop.hive.metastore.api.SFile;

public interface IDatabakRPC extends Remote {
	SFile getSFileById(long id) throws RemoteException;
}
