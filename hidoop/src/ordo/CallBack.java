package ordo;
import java.rmi.*;

interface CallBack extends Remote {
	public void callback(String URL) throws RemoteException;
	
}
