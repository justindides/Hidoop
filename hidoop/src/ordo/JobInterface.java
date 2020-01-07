package ordo;

import map.MapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;

public interface JobInterface {
// Méthodes requises pour la classe Job  
	public void setInputFormat(Format.Type ft);

	public void setInputFname(String fname);

	public void startJob(MapReduce mr);
}