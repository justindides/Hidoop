package hdfs;

import java.rmi.Naming;
import java.util.HashMap;
import java.util.Properties;

import config.Project;
import formats.Format;
import ordo.NameNodeInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;

public class HdfsClient {

	//private static final long serialVersionUID = 1L;
	private static Project structure = new Project();
	static String nameNodeURL;
	
	private static int nbNodes;
	
	private static int tailleMaxFragment = 100;
	
	private static int[] ports = {8000, 8001};

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {
    	
    	// Voir quel node possede le fichier (consultation du namenode)
    	// pour utiliser la bonne connexion. 
    	Commande cmd = new Commande(Commande.Cmd.CMD_DELETE, hdfsFname, 0);
    	//connexions[x].send(cmd);
    	
    }
	
    /*****************WRITE********************/
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) { 
    	
    	File f = new File(Project.PATH + localFSSourceFname);
    	/* On récupère le nom du fichier sans l'extension (.txt en general) */
    	String fSansExtension = localFSSourceFname.replaceFirst("[.][^.]+$", "");
    	long tailleFichier = f.length();
    	
    	Commande cmd = new Commande(Commande.Cmd.CMD_WRITE, "", 0);
    	
    	/* Buffer d'envoi de texte */
    	char[] buf = new char[tailleMaxFragment];
    	
    	int nbFragment = (int) (tailleFichier / tailleMaxFragment);
    	int reste = (int) (tailleFichier % tailleMaxFragment);
    	System.out.println("Nombre de fragments requis : " + nbFragment);
    	System.out.println("Caractère restant : " + reste);

    	
    	/* On aura besoin d'un fragment de plus s'il y a un reste à la divison */
    	if(reste != 0) {
    		System.out.println("Reste différent de 0 donc ajout d'un fragment.");
    		nbFragment ++;
    	}
    	/* Ce buffer servira à transmettre le dernier fragments qui n'aura pas un nombre fixe
    	 * de caractere.
    	 */
    	char[] miniBuf = new char[reste];
    	

    	try {
        	/* On va utiliser un file reader/writer sur les fichiers pour pouvoir lire leur contenu
        	 * et le mettre dans un buffer avant de l'envoyer. */
			FileReader fr = new FileReader(f);
			/* Donne le numéro de node sur lequel on écrit un fragment. */
			int node = 0; 
			int i;
			for(i = 0; i < nbFragment; i++) {
		    	Socket sock = new Socket("Localhost", ports[node]);
		    	Connexion c = new Connexion(sock);
				System.out.println("Ecriture du fragment n°" + i + " sur le node " + node);
				System.out.println("Lecture...");
				
				int ret = fr.read(buf, 0, tailleMaxFragment);
			
				if(ret != tailleMaxFragment) {
					System.out.println("Fin du fichier atteinte, écriture du dernier fragment");
					System.out.println("Envoi de la commande d'écriture...");

					cmd = new Commande(Commande.Cmd.CMD_WRITE, fSansExtension + "-bloc" + i, reste);
					c.send(cmd);
				/* On copie la dernière lecture dans le buffer plus petit ayant une taille adapté au nombres de caractère restants. */
					System.arraycopy(buf, 0, miniBuf, 0, reste);
					System.out.println("Envoi du fragment...");
					c.send(miniBuf);
				} else {
					System.out.println("Envoi de la commande d'écriture...");
					/* Envoi de la commande  */
					cmd = new Commande(Commande.Cmd.CMD_WRITE,  fSansExtension + "-bloc" + i, tailleMaxFragment);
					c.send(cmd);
					System.out.println("Envoi du fragment...");
					c.send(buf);
				}
				
				node ++;
				if(node == nbNodes) {
					node = 0;
				}
				c.Close();
			}
			System.out.println("Fin de l'écriture " + i + " fragment écrits.");
    	
    	} catch(IOException e) {
    		e.printStackTrace();
    	}
    }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) { }
    
    /** Fonction permettant de se connecter à tous les serveurs HDFS (ouverture des socekts).
     * 
     * @param nbServ Le nombre de serveur à notre disposition. On en déduira aussi leur numéros de port.
     */
    
	public static void getStructure() throws InvalidPropertyException {

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

			String property = propConf.getProperty("fileName1");

			for (int i = 1; property != null; i++) {
				property = propConf.getProperty("fileName" + i);
				numberOfFile = i - 1;
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		} finally {
			HashMap<Integer, String> listOfDaemons = new HashMap<Integer, String>();

			for (int i = 1; i <= numberOfFile; i++) {
				try {
					structure.numberOfMapsList.put(i,
							(new Integer(propConf.getProperty("numberOfMaps" + i))).intValue());
				} catch (NumberFormatException nfe) {
					System.out.println("Invalid Property about numberOfMaps" + i + " or NumberOfReduce" + i);
					System.exit(0);
				}
				structure.inputFileNameList.put(i, propConf.getProperty("fileName" + i));

			}
			for (int i = 1; propDaemons.getProperty("url" + i) != null; i++) {
				listOfDaemons.put(i, propDaemons.getProperty("url" + i));
			}

			structure.daemonsURLRepartized = repartition_des_blocs(listOfDaemons);

			for (int i = 1; i <= structure.numberOfMapsList.size(); i++) {
				if (structure.numberOfMapsList.get(i) == null) {
					throw new InvalidPropertyException("structure.numberOfMapsList.get(" + i + ")");
				}
			}
			for (int i = 1; i <= structure.inputFileNameList.size(); i++) {
				System.out.println(structure.inputFileNameList.get(i));
				if (structure.inputFileNameList.get(i) == null) {
					throw new InvalidPropertyException("structure.inputFileNameList.get(" + i + ")");
				}
			}
			for (int i = 1; i <= structure.daemonsURLRepartized.size(); i++) {
				for (int j = 1; j <= structure.daemonsURLRepartized.get(i).size(); j++) {
					if (structure.daemonsURLRepartized.get(i).get(j) == null) {
						throw new InvalidPropertyException(
								"structure.daemonsURLRepartized.get(" + i + ").get(" + j + ")");
					}
				}
			}
		}
	}
/*
	public Project getStructureMapReduce() {
		return structure;
	}
*/
	public static HashMap<Integer, HashMap<Integer, String>> repartition_des_blocs(HashMap<Integer, String> listOfDaemons) {

		HashMap<Integer, String> l = new HashMap<Integer, String>();
		HashMap<Integer, HashMap<Integer, String>> repartition_bloc = new HashMap<Integer, HashMap<Integer, String>>();

		// Fichier 1
		l.put(1, listOfDaemons.get(1));
		repartition_bloc.put(1, l);

		/*l.put(2, listOfDaemons.get(2));
		l.put(3, listOfDaemons.get(3));
		l.put(4, listOfDaemons.get(4));
		l.put(5, listOfDaemons.get(5));
		l.put(6, listOfDaemons.get(6));


		// fichier 2
		l = new HashMap<Integer, String>();
		l.put(1, listOfDaemons.get(3));
		l.put(2, listOfDaemons.get(4));

		repartition_bloc.put(2, l);

		// fichier 3
		l = new HashMap<Integer, String>();
		l.put(1, listOfDaemons.get(5));
		l.put(2, listOfDaemons.get(6));

		repartition_bloc.put(3, l);*/

		return repartition_bloc;

	}

	public static String getNamNodeURL() {
		String res = null;
		try {
			FileInputStream in = new FileInputStream("hidoop/data/hdfsClient/NameNode.url");
			Properties prop = new Properties();
			prop.load(in);
			in.close();

			res = prop.getProperty("url");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		return res;

	}

	
	public static void main(String[] args) {
		nbNodes = ports.length;
		try {
			
			 if (args.length<2) {usage(); return;}

	            switch (args[0]) {
	              case "read": HdfsRead(args[1],null); break;
	              case "delete": HdfsDelete(args[1]); break;
	              case "write": 
	            	  
	                Format.Type fmt;
	                if (args.length<3) {usage(); return;}
	                if (args[1].equals("line")) fmt = Format.Type.LINE;
	                else if(args[1].equals("kv")) fmt = Format.Type.KV;
	                else {usage(); return;}
	                HdfsWrite(fmt,args[2],1);
	            }  
	            
			getStructure();
			
			structure = new Project();

			NameNodeInterface nNI;
			try {
				nNI = (NameNodeInterface) Naming.lookup(getNamNodeURL());
				nNI.updateStructure(structure);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
			
			
		} catch (InvalidPropertyException e) {
			e.printStackTrace();
		}
        
        
	}

}
