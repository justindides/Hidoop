package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import java.net.*;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

import map.Mapper;
import formats.Format;
import formats.Format.OpenMode;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {

	private static final long serialVersionUID = 1L;
	String URL;

	public DaemonImpl() throws RemoteException {
	}

	public String getURL() throws RemoteException {
		return URL;
	}

	// pour git

	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		try {
			reader.open(OpenMode.R);
			writer.open(OpenMode.W);

			m.map(reader, writer);

			reader.close();
			writer.close();

			cb.callback(URL);

			System.out.println("Map executed");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		int port = 8000;

		Integer idInt = new Integer(args[0]);
		DaemonImpl dI;
		//////////////////////////////////////////// CREATION DU DAEMON
		//////////////////////////////////////////// ////////////////////////////////////////////
		try {
			dI = new DaemonImpl();
			dI.URL = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/Daemon" + idInt.intValue();

			try {
				Registry registry = LocateRegistry.getRegistry(port);
				Naming.rebind(dI.URL, dI);
				System.out.println("registry existant");

			} catch (Exception e) {
				try {
					System.out.println("creation registry");
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
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

}
