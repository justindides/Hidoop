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

	public void callback(String URL) throws RemoteException {
		System.out.println("Callback end of map: received message [" + URL + "]");

		if (daemonsURL.containsValue(URL)) {
			numberOfMapCallBack++;
		}

		if (numberOfMapCallBack == numberOfMaps) {
			System.out.println("L'ensemble des maps ont été exécuté, appel du read HDFS");
			try {
				Process read = Runtime.getRuntime().exec("java hidoop/bin/hdfs/HdfsClient read " + originalFname);
				read.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Read fini, appel du reduce");
			startReduce();
		}

	}

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
			//nn.setInputFname(originalFname);
			numberOfMaps = nn.getNumberOfMaps(originalFname);
			daemonsURL = nn.getDaemonsURL(originalFname);
		}
	}

	public void startJob(MapReduce mr) {

		mapReduce = mr;

		String URL;

		try {
			setProperties();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		String[] res = originalFname.split("[.]");

		for (int id = 0; id < numberOfMaps; id++) {

			inputFname = Project.PATH + res[0] + "-bloc" + id + "." + res[1];

			if (inputFormat == Format.Type.LINE) {
				inFormat = new LineFormat(inputFname);

			} else if (inputFormat == Format.Type.KV) {
				inFormat = new KVFormat(inputFname);
			}

			outputFname = Project.PATH + res[0] + "-res" + id + "." + res[1];
			outFormat = new KVFormat(outputFname);

			try {

				URL = daemonsURL.get(id);

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

		inputFname = Project.PATH + res[0] + "-concatenated." + res[1];
		inFormat = new KVFormat(inputFname);

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