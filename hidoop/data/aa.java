import java.io.File;
import java.io.IOException;

/** Pour tester les fonctions liées à des fichiers */
public class aa {

	public static void main(String[] args) {
		File f1 = new File("C:\\Users\\justin\\git\\hidoop\\hidoop\\bin\\bb.txt");
		File f2 = new File("C:\\Users\\justin\\git\\hidoop\\hidoop\\bin\\aa.txt"); // Il faut le chemin entier apparement
		
		try {
			f1.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(f2.length());
	}

}
