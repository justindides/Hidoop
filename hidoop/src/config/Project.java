package config;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Project est la classe contenant toute la configuration initiale du projet
 * ainsi que les informations devant �tre commune � Hidoop et HDFS,
 * principalement la cartographie des fragment sur les nodes. HDFS poss�de donc
 * un projet qu'il mettra � jour � chaque traitement, par exemple apr�s avoir
 * fragment� un fichier. Il mettra ensuite � jour par RMI le namenode qui
 * poss�de lui aussi un projet. Le namenode lui permet d'interm�diaire entre
 * HDFS et Hidoop, tenant � jour Hidoop sur les traitements de HDFS � fait.
 */
public class Project implements Serializable {

	private static final long serialVersionUID = 1L;
	public static String PATH = "/tmp/";

	/**
	 * Liste des adresses des Nodes pour les serveurs Hdfs, r�cup�r� par un fichier
	 * de config.
	 */
	public List<String> urlServ = new ArrayList<String>();

	/**
	 * Liste des URL des daemons Hidoop sur les nodes, r�cup�r� par un fichier de
	 * config.
	 */
	public List<String> urlDaemons = new ArrayList<String>();

	public List<Integer> portNodes = new ArrayList<Integer>();

	/** HashMap associant le fichier trait� � son nombre de frgament */
	public HashMap<String, Integer> numberOfMaps = new HashMap<String, Integer>();

	/**
	 * HashMap associant un fichier trait� par HDFS � une autre map contenant les
	 * couples : n� de bloc <-> URL du deamon le stockant. Cette donn�e �quivaut �
	 * la cartographie de la fragmentation d'un fichier en lien chacun de ses
	 * fragment � son daemon.
	 */
	public HashMap<String, HashMap<Integer, String>> daemonsFragmentRepartized = new HashMap<String, HashMap<Integer, String>>();

	/**
	 * Liste des fichiers initialement dans le syst�me de fichier HDFS. On les
	 * r�cup�re depuis un fichier de configuration qui permet de choisir quels
	 * fichiers nous vondront traiter avec hdfs.
	 */
	public List<String> inputFileNameList = new ArrayList<String>();

	public Project() {
		try {
			getStructure();
		} catch (InvalidPropertyException e) {
			e.printStackTrace();
		}

	}

	/**
	 * M�thode r�cup�rant la configuration du projet depuis les fichiers de config.
	 * On r�cup�re : - La liste des fichiers du syst�me pouvant �tre trait�s par
	 * HDFS. - Les URLs des daemons.
	 * 
	 * @throws InvalidPropertyException
	 */
	public void getStructure() throws InvalidPropertyException {

		Properties propConf = new Properties();
		Properties propDaemons = new Properties();
		Properties propPorts = new Properties();
		Properties propServ = new Properties();
		int numberOfFile = 0;

		try {
			FileInputStream isPorts = new FileInputStream("hidoop/data/hdfsClient/portNodes.conf");
			FileInputStream isConf = new FileInputStream("hidoop/data/hdfsClient/structure.conf");
			FileInputStream isDaemons = new FileInputStream("hidoop/data/hdfsClient/daemons.listofurl");
			FileInputStream isServ = new FileInputStream("hidoop/data/hdfsClient/servHdfs.listofurl");

			propDaemons.load(isDaemons);
			isDaemons.close();

			propServ.load(isServ);
			isServ.close();

			propConf.load(isConf);
			isConf.close();

			propPorts.load(isPorts);
			isPorts.close();

			String property = propConf.getProperty("fileName0");

			while (property != null) {
				numberOfFile++;
				property = propConf.getProperty("fileName" + numberOfFile);
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		} finally {
			/* R�cup�ration des noms de fichiers . */
			for (int i = 0; i < numberOfFile; i++) {
				inputFileNameList.add(propConf.getProperty("fileName" + i));
			}
			/* R�cup�ration des url des daemons. */
			int nbDaemons = 0;

			String daemon = propDaemons.getProperty("url0");

			while (daemon != null) {
				urlDaemons.add(daemon);
				// System.out.println("Config : Daemon " + nbDaemons + " recupere : " + daemon);
				nbDaemons++;
				daemon = propDaemons.getProperty("url" + nbDaemons);
			}

			int nbServ = 0;

			String serv = propServ.getProperty("url0");

			while (serv != null) {
				urlServ.add(serv);
				// System.out.println("Config : Serveur " + nbServ + " recupere : " + serv);
				nbServ++;
				serv = propServ.getProperty("url" + nbServ);
			}

			/* R�cup�ration des ports sockets des daemons. */
			int nbPorts = 0;

			String port = propPorts.getProperty("port0");

			while (port != null) {
				portNodes.add(Integer.parseInt(port));
				// System.out.println("Config : Port " + nbPorts + " recupere : " + port);
				nbPorts++;
				port = propPorts.getProperty("port" + nbPorts);
			}

			for (int i = 0; i < inputFileNameList.size(); i++) {
				System.out.println(inputFileNameList.get(i));
				if (inputFileNameList.get(i) == null) {
					throw new InvalidPropertyException("structure.inputFileNameList.get(" + i + ")");
				}
			}
		}
	}
}