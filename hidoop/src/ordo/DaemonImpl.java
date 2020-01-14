package ordo;

import java.rmi.RemoteException;

import java.net.*;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import map.Mapper;
import formats.Format;
import formats.Format.OpenMode;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {

	private static final long serialVersionUID = 1L;
	String URL;

	public DaemonImpl() throws RemoteException {
	}

	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		try {
			//Ouverture (ou creations) des fichiers de lecture et d'écriture
			reader.open(OpenMode.R);
			writer.open(OpenMode.W);

			//execution du map sur le fragment de fichier local
			m.map(reader, writer);

			//fermeture des fichiers de lecture et d'écriture
			reader.close();
			writer.close();
			
			//appel du callback après l'exécution du map
			cb.callback(URL);

			System.out.println("Map executed");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		//Choix du port du démon
		int port = Integer.parseInt(args[0]);

		//identifiant du démon (utile pour des test en local pour différencier les démons)
		Integer idInt = new Integer(args[1]);
		DaemonImpl dI;
		//////////////////////////////////////////// CREATION DU DAEMON
		//////////////////////////////////////////// ////////////////////////////////////////////
		try {
			dI = new DaemonImpl();
			dI.URL = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/Daemon" + idInt.intValue();

			//Code permettant de créer automatiquement le registry s'il n'éxiste pas 
			try {
				LocateRegistry.getRegistry(port);
				Naming.rebind(dI.URL, dI);
				System.out.println("Registry existant");

			} catch (Exception e) {
				try {
					System.out.println("Registry inexistant, creation du registry");
					LocateRegistry.createRegistry(port);
					Naming.rebind(dI.URL, dI);
				} catch (Exception ex) {
					ex.printStackTrace();
					System.exit(0);
				}
			}

			System.out.println("Daemon " + dI.URL + " bound in registry");

			//////////////////////////////////////////// FIN CREATION DU DEMON
			//////////////////////////////////////////// ////////////////////////////////////////////

		} catch (Exception e1) {
			e1.printStackTrace();
		}

	}

}
