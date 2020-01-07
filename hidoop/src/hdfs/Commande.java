package hdfs;

import java.io.Serializable;

/** Classe décrivant les commandes que le client envoie au serveur avant toute opération.
 * Elle est donc serializable car c'est un objet qu'on pourra envoyer sur la socket.
 * @author justin
 *
 */
public class Commande implements Serializable {

	/** Enumeration définissant les différentes types de commande que le client envoie au serveur. */
	public static enum  Cmd {CMD_READ , CMD_WRITE , CMD_DELETE };
	
	private Cmd cmd; 
	
	private int tailleTexte;
	
	private String nomFichier;
	
	public Commande(Cmd c, String nomF, int tailleTexte) {
		this.cmd = c;
		//this.tailleNom = tailleN;
		this.nomFichier = nomF;
		this.tailleTexte = tailleTexte;
	}
	
	/** Getter de la Cmd. 
	 * 
	 * @return La commande.
	 */
	public Cmd getCmd() {
		return this.cmd;
	}
	
	/** Getter du nom de fichier transmis.
	 * 
	 * @return le nom du fichier.
	 */
	public String getNomFichier() {
		return this.nomFichier;
	}
	
	/** Getter de la taille du texte (nombre de caractère transmis).
	 * 
	 */
	public int getTailleTexte() {
		return this.tailleTexte;
	}
	
}