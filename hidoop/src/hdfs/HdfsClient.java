package hdfs;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import config.InvalidPropertyException;
import config.Project;
import formats.Format;
import ordo.NameNodeInterface;

import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class HdfsClient {

	// private static final long serialVersionUID = 1L;
	/** Data contient toutes la configuration initiale du projet ainsi que les 
	 * informations qu'on a � donner � Hidoop.
	 */
	private static Project data;
	
	/** L'URL de la machine contenant le namenode. */
	private static String nameNodeURL;

	/** Nombre de daemons hidoop disponible */
	private static int nbNodes;

	/** D�finie la taille maximal d'un fragment lors de l'�criture. */
	private static int tailleMaxFragment = 20;
	
	/** Texte recu lors d'une lecture. */
	private static String strRecu;

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <line|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}

	public static void HdfsDelete(String hdfsFname) {

		// Voir quel node possede le fichier (consultation du namenode)
		// pour utiliser la bonne connexion.
		String fSansExtension = hdfsFname.replaceFirst("[.][^.]+$", "");

		HashMap<Integer, String> mappingBlocs = data.daemonsFragmentRepartized.get(fSansExtension);

		mappingBlocs.forEach((i, url) -> {
			Socket sock;
			try {
			/* Pour recuperer le port, on cherche l'indice d'url dans notre liste d'url. 
			 * Cette indice correspond au port dans la liste des ports.
			 */
				int nbNode = data.urlDaemons.indexOf(url);
				sock = new Socket(data.urlServ.get(nbNode), data.portNodes.get(nbNode));
				Connexion c = new Connexion(sock);
				Commande cmd = new Commande(Commande.Cmd.CMD_DELETE, fSansExtension + "-bloc" + i, 0);
				c.send(cmd);
				c.Close();
				// On supprime des data du projet le fragment.
				data.daemonsFragmentRepartized.get(fSansExtension).remove(i);
				System.out.println("Suppresion de " + fSansExtension + "-bloc" + i + " sur le node " + nbNode);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		data.daemonsFragmentRepartized.remove(fSansExtension);
	} 

	/***************** WRITE ********************/
	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {

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
		if (reste != 0) {
			System.out.println("Reste différent de 0 donc ajout d'un fragment.");
			nbFragment++;
		}
		/*
		 * Ce buffer servira à transmettre le dernier fragments qui n'aura pas un
		 * nombre fixe de caractere.
		 */
		char[] miniBuf = new char[reste];

		try {
			/*
			 * On va utiliser un file reader/writer sur les fichiers pour pouvoir lire leur
			 * contenu et le mettre dans un buffer avant de l'envoyer.
			 */
			FileReader fr = new FileReader(f);
			/* Donne le numéro de node sur lequel on écrit un fragment. */
			int node = 0;
			int i;
			/* Liste contenant tous les noms de fragments */
			HashMap<Integer, String> mappingBlocs = new HashMap<Integer, String>();
			
			for (i = 0; i < nbFragment; i++) {
				System.out.println("NbNode sock : " + node);
				Socket sock = new Socket(data.urlServ.get(node), data.portNodes.get(node));
				Connexion c = new Connexion(sock);
				System.out.println("Ecriture du fragment n " + i + " sur le node " + node);
				System.out.println("Lecture...");

				int ret = fr.read(buf, 0, tailleMaxFragment);

				if (ret != tailleMaxFragment) {
					System.out.println("Fin du fichier atteinte, ecriture du dernier fragment");
					System.out.println("Envoi de la commande d'ecriture...");

					cmd = new Commande(Commande.Cmd.CMD_WRITE, fSansExtension + "-bloc" + i, reste);
					c.send(cmd);
					/*
					 * On copie la dernière lecture dans le buffer plus petit ayant une taille
					 * adapté au nombres de caractère restants.
					 */
					System.arraycopy(buf, 0, miniBuf, 0, reste);
					System.out.println("Envoi du fragment...");
					c.send(miniBuf);
				} else {
					System.out.println("Envoi de la commande d'ecriture...");
					/* Envoi de la commande */
					cmd = new Commande(Commande.Cmd.CMD_WRITE, fSansExtension + "-bloc" + i, tailleMaxFragment);
					c.send(cmd);
					System.out.println("Envoi du fragment...");
					c.send(buf);
				}
				
				/* Association du fragment au node correspondant */
				mappingBlocs.put(i, data.urlDaemons.get(node));
				
				node++;
				if (node == nbNodes) {
					node = 0;
				}
				c.Close();
			}
			System.out.println("Fin de l'ecriture " + i + " fragment ecrits.");
			data.daemonsFragmentRepartized.put(fSansExtension, mappingBlocs);
			
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
		// Voir quel node possede le fichier (consultation du namenode)
		// pour utiliser la bonne connexion.
		String fSansExtension = hdfsFname.replaceFirst("[.][^.]+$", "");

		HashMap<Integer, String> mappingBlocs = data.daemonsFragmentRepartized.get(fSansExtension);

		/* Fichier dans lequel on �crira le r�sultat de la lecture */
		localFSDestFname = data.PATH + fSansExtension + "_recover";
		File f = new File(localFSDestFname);
		FileWriter fw;

		try {
			strRecu = new String();
			
			fw = new FileWriter(f);

			/* Pour chaque fragment, on poss�de l'URL du node le stockant. */
			mappingBlocs.forEach((i, url) -> {

				try {
					int nbNode = data.urlDaemons.indexOf(url);
				/* On ouvre une connexion poura chaque fragment, on lie, on le concatene � strRecu */
					Socket sock = new Socket(data.urlServ.get(nbNode), data.portNodes.get(nbNode));
					Connexion c = new Connexion(sock);
					
				/* Rappel : le nom d'un fragment est nom_du_fichier-blocx avec x num�ro du fragment. */
					Commande cmd = new Commande(Commande.Cmd.CMD_READ, fSansExtension + "-bloc" + i, 0);
					c.send(cmd);
					
					// On concatene les textes de tous les fragments.
					strRecu = strRecu + (String) c.receive();
					
					System.out.println("Lecture du fragment " + fSansExtension + "-bloc" + i + " sur le node " + nbNode);
	
					c.Close();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			fw.write(strRecu);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static String getNamNodeURL() {
		String res = null;
		try {
			FileInputStream in = new FileInputStream("hidoop/data/hdfsClient/namenode.url");
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
		
		try {
			NameNodeInterface nNI;

			nNI = (NameNodeInterface) Naming.lookup(getNamNodeURL());
			
			if(nNI.structureExists()) {
				System.out.println("Namenode existant : Recuperation.");
				data = nNI.recoverStructure();
			} else {
				System.out.println("Pas de namenode trouvé, création d'un nouveau.");
				data = new Project();
			}
			
			nbNodes = data.urlServ.size();

			if (args.length < 2) {
				usage();
				return;
			}

			switch (args[0]) {
			case "read":
				HdfsRead(args[1], null);
				break;
			case "delete":
				HdfsDelete(args[1]);
				break;
			case "write":

				Format.Type fmt;
				if (args.length < 3) {
					usage();
					return;
				}
				if (args[1].equals("line"))
					fmt = Format.Type.LINE;
				else if (args[1].equals("kv"))
					fmt = Format.Type.KV;
				else {
					usage();
					return;
				}
				HdfsWrite(fmt, args[2], 1);
			}

			/* Mise � jour du namenode */
			nNI.updateStructure(data);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

}
