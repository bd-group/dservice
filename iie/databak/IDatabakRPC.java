package iie.databak;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.Table;

public interface IDatabakRPC extends Remote {
	SFile getSFileById(long id) throws RemoteException;
	Table getTable(String dbname, String tablename) throws RemoteException;
}
