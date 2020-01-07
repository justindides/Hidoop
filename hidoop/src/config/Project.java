package config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Project implements Serializable {

	private static final long serialVersionUID = 1L;
	public static String PATH = "hidoop/data/";

	public HashMap<Integer, Integer> numberOfMapsList = new HashMap<Integer, Integer>();
	public HashMap<Integer, HashMap<Integer, String>> daemonsURLRepartized = new HashMap<Integer, HashMap<Integer, String>>();
	public HashMap<Integer, String> inputFileNameList = new HashMap<Integer, String>();


}