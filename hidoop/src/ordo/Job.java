// une *proposition*, qui  peut être complétée, élaguée ou adaptée

package ordo;

import map.MapReduce;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;

import config.Project;
import formats.Format;

import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import formats.Format.OpenMode;

public class Job extends UnicastRemoteObject implements JobInterface, JobInterfaceX, CallBack {

	private static final long serialVersionUID = 1L;

	private static int numberOfReduces;
	private static int numberOfMaps;
	private static Format.Type inputFormat;
	private static Format.Type outputFormat;
	private static String originalFname;
	private static String inputFname;
	private static String outputFname;
	private static SortComparator sortComparator;
	private static Format inFormat;
	private static Format outFormat;
	private static Daemon d;
	private static HashMap<Integer, String> daemonsURL;
	private static int numberOfMapCallBack = 0;
	private static MapReduce mapReduce;

	public Job() throws RemoteException {

	}

	//Implémentation du callback :
	public void callback(String URL) throws RemoteException {
		System.out.println("Callback end of map: received message from [" + URL + "]");

		//indentation du nombre de callback reçu :
		if (daemonsURL.containsValue(URL)) {
			numberOfMapCallBack++;
		}

		//Si Job à reçu l'ensemble des callbacks :
		if (numberOfMapCallBack == numberOfMaps) {
			System.out.println("L'ensemble des maps ont été exécuté, appel du read HDFS");
			
			String[] args = {"read", originalFname};
			HdfsClient.main(args);
			System.out.println("Read fini, appel du reduce");
			startReduce();
		}

	}

	//Récupération des properties des fichiers de configuration par appel au namenode:
	public void setProperties() throws RemoteException, WrongFileNameException {
		NameNodeInterface nn = null;

		try {
			FileInputStream in = new FileInputStream("hidoop/data/job/namenode.url");
			Properties prop = new Properties();
			prop.load(in);
			in.close();
			String nameNodeURL = prop.getProperty("url");

			nn = (NameNodeInterface) Naming.lookup(nameNodeURL);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		} finally {
			daemonsURL = new HashMap<Integer, String>();
			numberOfMaps = nn.getNumberOfMaps(originalFname);
			daemonsURL = nn.getDaemonsURL(originalFname);
		}
	}

	public void startJob(MapReduce mr) {

		mapReduce = mr;

		String URL;

		try {
			//Récupération des properties des fichiers de configuration :
			setProperties();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		//Récuparation du nom de fichier sans l'extension
		String[] res = originalFname.split("[.]");

		for (int id = 0; id < numberOfMaps; id++) {
			//Création des noms de fichiers blocs :
			inputFname = Project.PATH + res[0] + "-bloc" + id + "." + res[1];

			//Définition du format de fichier :
			if (inputFormat == Format.Type.LINE) {
				inFormat = new LineFormat(inputFname);
			} else if (inputFormat == Format.Type.KV) {
				inFormat = new KVFormat(inputFname);
			}

			//Génération du nom de fichier résultat
			outputFname = Project.PATH + res[0] + "-res" + id + "." + res[1];
			outFormat = new KVFormat(outputFname);

			try {

				URL = daemonsURL.get(id);
				
				//Appel du démon :
				System.out.println("Lancement map : " + URL);
				d = (Daemon) Naming.lookup(URL);
				d.runMap(mr, inFormat, outFormat, new Job());

			} catch (NotBoundException nbe) {
				System.out.println("No daemons available");
				System.exit(0);
			} catch (RemoteException re) {
				System.out.println("RMI Error - " + re);
				System.exit(0);
			} catch (Exception e) {
				System.out.println("Error - " + e);
				System.exit(0);
			}
		}
	}

	public void startReduce() {

		String[] res = originalFname.split("[.]");

		//Nom normalisé du fichier d'entré au réduce :
		inputFname = Project.PATH + res[0] + "-concatenated." + res[1];
		inFormat = new KVFormat(inputFname);

		//Nom du fichier de résultat :
		outputFname = Project.PATH + res[0] + "-res." + res[1];
		outFormat = new KVFormat(outputFname);

		
		inFormat.open(OpenMode.R);
		outFormat.open(OpenMode.W);

		mapReduce.reduce(inFormat, outFormat);

		inFormat.close();
		outFormat.close();

	}

	public void setInputFormat(Format.Type ft) {
		inputFormat = ft;
	}

	public void setOutputFormat(Format.Type ft) {
		outputFormat = ft;
	}

	public void setInputFname(String fname) {
		originalFname = fname;
	}

	public void setOutputFname(String fname) {
		outputFname = fname;
	}

	public void setSortComparator(SortComparator sc) {
		sortComparator = sc;
	}

	public int getNumberOfMaps() {
		return numberOfMaps;
	}

	public Format.Type getInputFormat() {
		return inputFormat;
	}

	public Format.Type getOutputFormat() {
		return outputFormat;
	}

	public String getInputFname() {
		return originalFname;
	}

	public String getOutputFname() {
		return outputFname;
	}

	public SortComparator getSortComparator() {
		return sortComparator;
	}
}