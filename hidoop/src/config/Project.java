package config;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/** Project est la classe contenant toute la configuration initiale du projet ainsi que les 
* informations devant ï¿½tre commune ï¿½ Hidoop et HDFS, principalement la cartographie des fragment
* sur les nodes. HDFS possï¿½de donc un projet qu'il mettra ï¿½ jour ï¿½ chaque traitement, par exemple
*  aprï¿½s avoir fragmentï¿½ un fichier. Il mettra ensuite ï¿½ jour par RMI le namenode qui possï¿½de lui aussi un projet.
*  Le namenode lui permet d'intermï¿½diaire entre HDFS et Hidoop, tenant ï¿½ jour Hidoop sur les traitements de HDFS ï¿½ fait.
*/
public class Project implements Serializable {

	private static final long serialVersionUID = 1L;
	public static String PATH = "hidoop/data/";

	/** Liste des URL des daemons, rï¿½cupï¿½rï¿½ par un fichier de config. */ 
	public List<String> urlNodes = new ArrayList<String>();
	
	public List<Integer> portNodes = new ArrayList<Integer>();
	
	/** HashMap associant le fichier traité à son nombre de frgament */
	public HashMap<String,Integer> numberOfMaps;
	
	/** HashMap associant un fichier traitï¿½ par HDFS ï¿½ une autre map contenant 
	 * les couples : nï¿½ de bloc <-> URL du deamon le stockant. Cette donnï¿½e ï¿½quivaut ï¿½ la cartographie 
	 * de la fragmentation d'un fichier en lien chacun de ses fragment ï¿½ son daemon. 
	 */
	public HashMap<String, HashMap<Integer, String>> daemonsFragmentRepartized = new HashMap<String, HashMap<Integer, String>>();
	
	/** Liste des fichiers initialement dans le systï¿½me de fichier HDFS. On les rï¿½cupï¿½re depuis un fichier
	 * de configuration qui permet de choisir quels fichiers nous vondront traiter avec hdfs.
	 */
	public List<String> inputFileNameList = new ArrayList<String>();
	
	public Project() {
		try {
			getStructure();
		} catch(InvalidPropertyException e) {
			e.printStackTrace();
		}

	}
	
	/** Mï¿½thode rï¿½cupï¿½rant la configuration du projet depuis les fichiers de config.
	 *  On rï¿½cupï¿½re : - La liste des fichiers du systï¿½me pouvant ï¿½tre traitï¿½s par HDFS.
	 *  - Les URLs des daemons.
	 * 
	 * @throws InvalidPropertyException
	 */
	public void getStructure() throws InvalidPropertyException {

		Properties propConf = new Properties();
		Properties propDaemons = new Properties();
		Properties propPorts = new Properties();
		int numberOfFile = 0;

		try {
			FileInputStream isPorts = new FileInputStream("hidoop/data/hdfsclient/portDaemons.conf");
			FileInputStream isConf = new FileInputStream("hidoop/data/hdfsClient/structure.conf");
			FileInputStream isDaemons = new FileInputStream("hidoop/data/hdfsClient/daemons.listofurl");

			propDaemons.load(isDaemons);
			isDaemons.close();

			propConf.load(isConf);
			isConf.close();
			
			propPorts.load(isPorts);
			isPorts.close();

			String property = propConf.getProperty("fileName0");
			
			while(property != null) {
				numberOfFile++;
				property = propConf.getProperty("fileName" + numberOfFile);
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		} finally {
			/* Rï¿½cupï¿½ration des noms de fichiers . */ 
			for (int i = 0; i < numberOfFile; i++) {
				inputFileNameList.add(propConf.getProperty("fileName" + i));
			}
			/* Rï¿½cupï¿½ration des url des daemons. */
			int nbDaemons = 0;
			
			String daemon = propDaemons.getProperty("url0");

			while(daemon != null) {
				urlNodes.add(daemon);
				System.out.println("Config : Daemon " + nbDaemons + " recupere : " + daemon);
				nbDaemons++;
				daemon = propConf.getProperty("url" + nbDaemons);
			}
			
			/* Récupération des ports sockets des daemons. */
			String port = propPorts.getProperty("port0");
			int nbPorts = 0;
			
			while(port != null) {
				portNodes.add(Integer.parseInt(port));
				System.out.println("Config : Port " + nbPorts + " recupere : " + port);
				nbPorts++;
				port = propPorts.getProperty("port" + nbPorts);
			}

			for (int i = 0; i < inputFileNameList.size(); i++) {
				System.out.println(inputFileNameList.get(i));
				if (inputFileNameList.get(i) == null) {
					throw new InvalidPropertyException("structure.inputFileNameList.get(" + i + ")");
				}
			}
			/*
			for (int i = 1; i <= daemonsFragmentRepartized.size(); i++) {
				for (int j = 1; j <= daemonsFragmentRepartized.get(i).size(); j++) {
					if (daemonsFragmentRepartized.get(i).get(j) == null) {
						throw new InvalidPropertyException(
								"structure.daemonsURLRepartized.get(" + i + ").get(" + j + ")"); 
					} 
				}
			} */
		}
	}
}