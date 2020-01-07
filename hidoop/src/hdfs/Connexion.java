package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

/** Classe permettant d'utiliser la connexion (par une socket) entre un client et un serveur.
 * Pour cela, on utilise les input et output stream. Par simplicité, on utilisera des Object out/input stream.
 * 
 * @author justin
 *
 */
public class Connexion {
	
	/** La socket de connexion */
	private Socket sock; 
	/** Le stream Client -> Serveur. */
	private ObjectOutputStream oos;
	/** Le stream Serveur -> Client. */
	private ObjectInputStream ois;

	/** Constructeur
	 * On lui des donne les streams en paramètres.
	 * 
	 * @param socket La socket de connexion.
	 *  */
	public Connexion(Socket socket) {
		this.sock = socket;
		try {
			this.oos = new ObjectOutputStream(sock.getOutputStream());
			this.ois = new ObjectInputStream(sock.getInputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/** Méthode d'envoi d'un objet.
	 * 
	 * @param objet L'objet à envoyer
	 *  */
	public void send(Object objet) {
		try {
			oos.writeObject(objet);
		} catch (UnknownHostException e) {
			System.out.println("Host inconnui à l'envoi");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/** Méthode de réception d'un objet.
	 * 
	 * @return l'Objet recut
	 *  */
	public Object receive() {
		Object objet = null;
		try {
			objet = (Object) ois.readObject();
		} catch (UnknownHostException e) {
			System.out.println("Host non reconnu à la réception.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("Objet inconnu à la réception");
			e.printStackTrace();
		}
		return objet;
	}
	
	public void Close() {
		try {
			sock.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}