package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

import config.Project;


public interface NameNodeInterface extends Remote {

	public int getNumberOfMaps(String inputFname) throws RemoteException;

	public HashMap<Integer, String> getDaemonsURL(String inputFname) throws RemoteException;

	//public void setInputFname(String inputFname) throws RemoteException, WrongFileNameException;
	
//	public String getIntputFileName() throws RemoteException ;
	
	public void updateStructure(Project struct) throws RemoteException;

}
