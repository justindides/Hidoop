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
	/**
	 * Data contient toutes la configuration initiale du projet ainsi que les
	 * informations qu'on a � donner � Hidoop.
	 */
	private static Project dataStructure;

	/** L'URL de la machine contenant le namenode. */
	private static String nameNodeURL;

	/** Nombre de daemons hidoop disponible */
	private static int nbNodes;

	/** D�finie la taille maximal d'un fragment lors de l'�criture. */
	private static int tailleMaxFragment = 100;

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

		HashMap<Integer, String> mappingBlocs = dataStructure.daemonsFragmentRepartized.get(fSansExtension);

		mappingBlocs.forEach((i, url) -> {
			Socket sock;
			try {
				/*
				 * Pour recuperer le port, on cherche l'indice d'url dans notre liste d'url.
				 * Cette indice correspond au port dans la liste des ports.
				 */
				int nbNode = dataStructure.urlDaemons.indexOf(url);
				sock = new Socket(dataStructure.urlServ.get(nbNode), dataStructure.portNodes.get(nbNode));
				Connexion c = new Connexion(sock);
				Commande cmd = new Commande(Commande.Cmd.CMD_DELETE, fSansExtension + "-bloc" + i + ".txt", 0);
				c.send(cmd);
				c.Close();
				// On supprime des data du projet le fragment.
				dataStructure.daemonsFragmentRepartized.get(fSansExtension).remove(i);
				System.out.println("Suppresion de " + fSansExtension + "-bloc" + i + " sur le node " + nbNode);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		dataStructure.daemonsFragmentRepartized.remove(fSansExtension);
	}

	/***************** WRITE ********************/
	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {

		File f = new File(Project.PATH + localFSSourceFname);
		/* On récupère le nom du fichier sans l'extension (.txt en general) */
		String fSansExtension = localFSSourceFname.replaceFirst("[.][^.]+$", "");
		long tailleFichier = f.length();
		System.out.println("Taille du fichier à traiter :" + tailleFichier);
		Commande cmd = new Commande(Commande.Cmd.CMD_WRITE, "", 0);

		/*
		 * Buffer d'envoi de texte On veut que le dernier caract�re soit un espace ' ',
		 * ou la fin du fichier pour ne pas couper un mot en 2. On Tronquera donc la fin
		 * du buffer jusqu'� ce qu'elle corresponde � un espace.
		 */
		char[] buf = new char[tailleMaxFragment];
		int nbCaracEnvoye;
		/*
		 * Respr�sente le nombre de caract�re qu'on enleve du buffer avant de rencontre
		 * un ' '.
		 */
		int nbCaracPerdu = 0;
		int j;

		/* Val de retour du read */
		int ret;

		int nbFragment = (int) (tailleFichier / tailleMaxFragment);
		int reste = (int) (tailleFichier % tailleMaxFragment);
		System.out.println("Nombre de fragments requis : " + nbFragment);
		System.out.println("Caractère restant : " + reste);

		/* On aura besoin d'un fragment de plus s'il y a un reste à la divison */
		if (reste != 0) {
			System.out.println("Reste différent de 0 donc ajout d'un fragment.");
			nbFragment++;
		}

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

			// Envoi des fragments.
			for (i = 0; i < nbFragment; i++) {
				// On enlève le // du début de l'adresse 
				String addresseNode = dataStructure.urlServ.get(node);
				addresseNode = addresseNode.substring(2, addresseNode.length());
				Socket sock = new Socket(addresseNode, dataStructure.portNodes.get(node));
				Connexion c = new Connexion(sock);
				System.out.println("Ecriture du fragment n " + i + " sur le node " + node);
				System.out.println("Lecture...");

				/*
				 * On repalce les caract�res tronqu�s au dernier fragment en d�but de buffer.
				 * Sur la ligne au dessus, on voit que l'on r�serve le d�but du buffer � ces
				 * caract�res en commencant le stockage plus loin.
				 */
				for (int l = 0; l < nbCaracPerdu; l++) {
					buf[l] = buf[tailleMaxFragment - nbCaracPerdu + l];
				}

				ret = fr.read(buf, nbCaracPerdu, tailleMaxFragment - nbCaracPerdu);

				j = tailleMaxFragment;
				nbCaracPerdu = 0;
				if (i != nbFragment) {
					while (buf[j - 1] != ' ' && j >= 1) {
						j--;
						nbCaracPerdu++;
					}
					/* On rajoute les caract�re non lus au reste */
					reste += nbCaracPerdu;
					/*
					 * Si le reste est sup�rieur � une taille de fragment, on rajoute un fragment.
					 */
					if (reste >= tailleMaxFragment) {
						reste -= tailleMaxFragment;
						nbFragment++;
					}
					nbCaracEnvoye = tailleMaxFragment - nbCaracPerdu;
					System.out.println("Lecture finie, fragment contenant : " + nbCaracEnvoye + " caract�res.");

				} else {
					nbCaracEnvoye = reste;
					System.out.println("Fin du fichier atteinte, dernier fragment � envoyer, de taille : "
							+ nbCaracEnvoye + " caract�res.");
				}

				System.out.println("perdu : " + nbCaracPerdu + " envoye : " + nbCaracEnvoye);
				cmd = new Commande(Commande.Cmd.CMD_WRITE, fSansExtension + "-bloc" + i + ".txt", nbCaracEnvoye);
				c.send(cmd);
				System.out.println("Envoi du fragment...");
				c.send(buf);

				/* Association du fragment au daemon correspondant */
				mappingBlocs.put(i, dataStructure.urlDaemons.get(node));
				node++;
				if (node == nbNodes) {
					node = 0;
				}
				c.Close();
			}

			dataStructure.numberOfMaps.put(localFSSourceFname, nbFragment);

			System.out.println("Fin de l'ecriture " + i + " fragment ecrits.");
			dataStructure.daemonsFragmentRepartized.put(localFSSourceFname, mappingBlocs);

			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
		// Voir quel node possede le fichier (consultation du namenode)
		// pour utiliser la bonne connexion.
		String fSansExtension = hdfsFname.replaceFirst("[.][^.]+$", "");

		HashMap<Integer, String> mappingBlocs = dataStructure.daemonsFragmentRepartized.get(hdfsFname);

		/* Fichier dans lequel on �crira le r�sultat de la lecture */
		localFSDestFname = dataStructure.PATH + fSansExtension + "-concatenated.txt";
		File f = new File(localFSDestFname);
		FileWriter fw;

		try {
			strRecu = new String();

			fw = new FileWriter(f);

			/* Pour chaque fragment, on poss�de l'URL du node le stockant. */
			mappingBlocs.forEach((i, url) -> {

				try {
					int nbNode = dataStructure.urlDaemons.indexOf(url);
					/*
					 * On ouvre une connexion poura chaque fragment, on lie, on le concatene �
					 * strRecu
					 */
					
					// On enlève le // du début de l'adresse 
					String addresseNode = dataStructure.urlServ.get(nbNode);
					addresseNode = addresseNode.substring(2, addresseNode.length());
					Socket sock = new Socket(addresseNode, dataStructure.portNodes.get(nbNode));
					Connexion c = new Connexion(sock);

					/*
					 * Rappel : le nom d'un fragment est nom_du_fichier-blocx avec x num�ro du
					 * fragment.
					 */
					Commande cmd = new Commande(Commande.Cmd.CMD_READ, fSansExtension + "-res" + i + ".txt", 0);
					c.send(cmd);

					// On concatene les textes de tous les fragments.
					strRecu = strRecu + (String) c.receive();

					System.out.println("Lecture du fragment " + fSansExtension + "-res" + i + " sur le node " + nbNode);

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

			if (nNI.structureExists()) {
				System.out.println("Namenode existant : Recuperation.");
				dataStructure = nNI.recoverStructure();
			} else {
				System.out.println("Pas de namenode trouvé, création d'un nouveau.");
				dataStructure = new Project();
			}

			nbNodes = dataStructure.urlServ.size();

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
			System.out.println(dataStructure.numberOfMaps.get(args[1]));
			nNI.updateStructure(dataStructure);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

}
