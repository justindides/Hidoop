package ordo;


import java.net.*;

import config.Project;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.server.*;

import java.util.HashMap;

public class NameNode extends UnicastRemoteObject implements NameNodeInterface {

	private static final long serialVersionUID = 1L;
	private static String URL;
	//private static String hdfsClientURL;
	private Project structure;
	private static int port = 8000;
	//private int idFile;

	public NameNode() throws RemoteException {
	}
	
	/** Setter de la structure du projet */
	public void updateStructure(Project struct) throws RemoteException {
		structure = struct;
		System.out.println("Update structure : Reception d'une nouvelle structure par HDFSClient.");
	}
	/** Getter de la structure du projet */
	public Project recoverStructure() throws RemoteException {
		System.out.println("Requete de récuperation de structure par HDFSClient");
		return structure;
	}

	public boolean structureExists() throws RemoteException{
		return (structure != null);
	}
/*	public void setInputFname(String inputFname) throws RemoteException, WrongFileNameException {
		idFile = 1;

		for (int i = 1; i <= structure.inputFileNameList.size(); i++) {
			if (structure.inputFileNameList.get(i).equals(inputFname))
				break;
			else
				idFile++;

		}

		if (idFile > structure.inputFileNameList.size()) {
			throw new WrongFileNameException();
		}
	}*/

	public int getNumberOfMaps(String inputFname) throws RemoteException {
		return structure.numberOfMaps.get(inputFname);
	}

	public HashMap<Integer, String> getDaemonsURL(String inputFname) throws RemoteException {
		return structure.daemonsFragmentRepartized.get(inputFname);
	}
	
/*	public String getIntputFileName() throws RemoteException {
		return structure.inputFileNameList.get(idFile);
	}*/

/*	public static void getHDFSClientURL() {
		try {
			FileInputStream in = new FileInputStream("hidoop/data/namenode/hdfsclient.url");
			Properties prop = new Properties();
			prop.load(in);
			in.close();

			hdfsClientURL = prop.getProperty("url");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}

	}*/

	public static void main(String args[]) {

		//getHDFSClientURL();

		try {
			NameNode nn = new NameNode();

			URL = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/NameNode";

			try {
				LocateRegistry.getRegistry(port);
				Naming.rebind(URL, nn);
				System.out.println("NameNode create at URL : " + URL + ", registry existant");

			} catch (Exception e) {
				try {
					LocateRegistry.createRegistry(port);
					Naming.rebind(URL, nn);
					System.out.println("NameNode create at URL : " + URL + ", création registry");

				} catch (Exception ex) {
					ex.printStackTrace();
					System.exit(0);
				}
			}

		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(0);
		}

	}

}
