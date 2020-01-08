package config;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/** Project est la classe contenant toute la configuration initiale du projet ainsi que les 
* informations devant être commune à Hidoop et HDFS, principalement la cartographie des fragment
* sur les nodes. HDFS possède donc un projet qu'il mettra à jour à chaque traitement, par exemple
*  après avoir fragmenté un fichier. Il mettra ensuite à jour par RMI le namenode qui possède lui aussi un projet.
*  Le namenode lui permet d'intermédiaire entre HDFS et Hidoop, tenant à jour Hidoop sur les traitements de HDFS à fait.
*/
public class Project implements Serializable {

	private static final long serialVersionUID = 1L;
	public static String PATH = "hidoop/data/";

	/** Liste des URL des daemons, récupéré par un fichier de config. */ 
	public List<String> urlNodes = new ArrayList<String>();
	
	/** HashMap associant le fichier traité à son nombre de frgament */
	public HashMap<String,Integer> numberOfMaps;
	
	/** HashMap associant un fichier traité par HDFS à une autre map contenant 
	 * les couples : n° de bloc <-> URL du deamon le stockant. Cette donnée équivaut à la cartographie 
	 * de la fragmentation d'un fichier en lien chacun de ses fragment à son daemon. 
	 */
	public HashMap<String, HashMap<Integer, String>> daemonsFragmentRepartized = new HashMap<String, HashMap<Integer, String>>();
	
	/** Liste des fichiers initialement dans le système de fichier HDFS. On les récupère depuis un fichier
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
	
	/** Méthode récupérant la configuration du projet depuis les fichiers de config.
	 *  On récupère : - La liste des fichiers du système pouvant être traités par HDFS.
	 *  - Les URLs des daemons.
	 * 
	 * @throws InvalidPropertyException
	 */
	public void getStructure() throws InvalidPropertyException {

		Properties propConf = new Properties();
		Properties propDaemons = new Properties();
		int numberOfFile = 0;

		try {
			FileInputStream isConf = new FileInputStream("hidoop/data/hdfsclient/structure.conf");
			FileInputStream isDaemons = new FileInputStream("hidoop/data/hdfsclient/daemons.listofurl");

			propDaemons.load(isDaemons);
			isDaemons.close();

			propConf.load(isConf);
			isConf.close();

			String property = propConf.getProperty("fileName0");
			
			while(property != null) {
				numberOfFile++;
				property = propConf.getProperty("fileName" + numberOfFile);
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		} finally {
			/* Récupération des noms de fichiers . */ 
			for (int i = 1; i <= numberOfFile; i++) {
				inputFileNameList.add(propConf.getProperty("fileName" + i));
			}
			/* Récupération des url des daemons. */
			int nbDaemons = 0;
			
			String daemon = propDaemons.getProperty("url0");

			while(daemon != null) {
				urlNodes.add(daemon);
				nbDaemons++;
				daemon = propConf.getProperty("url" + nbDaemons);
			}


			for (int i = 1; i <= inputFileNameList.size(); i++) {
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