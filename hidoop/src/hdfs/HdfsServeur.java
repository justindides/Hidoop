package hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import config.Project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Classe d'un serveur HDFS = DataNode hdfs qui acceuillera les fragments de
 * fichier. Il prendra en argument son numÃ©ro de Node
 * 
 * @author jdides
 *
 */
public class HdfsServeur extends Thread {

	/** Socket de communication avec le client */
	private static Socket s;

	public HdfsServeur(Socket s) throws IOException {
		HdfsServeur.s = s;
	}

	public void run() {
		Connexion c = new Connexion(s);

		System.out.println("Attente d'une commande...");
		Commande cmd = (Commande) c.receive();
		String fname = cmd.getNomFichier();
		File f = new File(Project.PATH + fname);

		switch (cmd.getCmd()) {

		case CMD_READ:
			// On recoit une commande de lecture
			System.out.println(" Demande de lecture reçue ... ");

			try {
				// Ouverture du fichier en lecture
				FileReader fr = new FileReader(f);
				BufferedReader br = new BufferedReader(fr);
				// Creation de la chaine de caractères qui sera envoyée
				String strToSend = new String();
				String line = br.readLine();
				while (line != null) {
					strToSend += line + "\n";
					line = br.readLine();
				}
				br.close();
				// Envoie de la chaine de caractère
				c.send(strToSend);
				System.out.println("fragment du fichier envoyé");
			} catch (FileNotFoundException fnfe) {
				System.out.println("fichier lu non existant");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;

		case CMD_WRITE:
			System.out.println("Demande d'ecriture recue ...");

			FileWriter fw;
			try {
				fw = new FileWriter(f);

				// Reception de la chaine de caractères correspondant au fragment
				char[] buf = new char[cmd.getTailleTexte()];

				buf = (char[]) c.receive();
				fw.write(buf, 0, cmd.getTailleTexte());

				// Fermeture du fichier
				fw.close();
				System.out.println("fragment " + cmd.getNomFichier() + " ecrit");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;

		case CMD_DELETE:
			System.out.print("Demande de suppression reçue ...");
			f.delete();
			System.out.println("fichier supprimé");

		default:
			break;
		}
	}

	public static void main(String[] args) {
		/* On recoit en argument le numÃ©ro de node du serveur */
		int port = Integer.parseInt(args[0]);

		try {
			/* Attente de la connexion du client */
			ServerSocket ss = new ServerSocket(port);
			System.out.println("Attente d'ouverture d'une socket ...");

			while (true) {
				s = ss.accept();
				new HdfsServeur(s).start();
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}