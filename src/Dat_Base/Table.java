package Dat_Base;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Table {

	private DBHelper dbHelper;
	private String tableName;
	private int pageCount;

	public Table(DBHelper dbHelper, String tableName) {
		this.dbHelper = dbHelper;
		this.tableName = tableName;
		this.pageCount = 0;
		// Create Folder
		createFolder(tableName);
	}
	public boolean createFolder(String name)
	{
		File theDir = new File("./data/table_name");

		// if the directory does not exist, create it
		boolean result = false;
		if (!theDir.exists()) {
		    System.out.println("creating directory: " + theDir.getName());

		    try{
		        theDir.mkdir();
		        result = true;
		    } 
		    catch(SecurityException se){
		        //handle it
		    }        
		    if(result) {    
		        System.out.println("DIR created");  
		    }
		}
		return result;
	}
//<<<<<<< HEAD
//=======
//
//>>>>>>> 086a7631be2aac84f306923a58b3b4599d7141cd
	@SuppressWarnings("resource")
	public Page readPage(String path) {
		try {
			FileInputStream fstream = new FileInputStream(path);
			ObjectInputStream ois = new ObjectInputStream(fstream);
			return (Page) ois.readObject();
		} catch (Exception e) {
			System.err.println("Error Reading from Page");
			e.printStackTrace(System.err);
		}
		return null;
	}

	public void writePage(String path, Page page) {
		try {
			FileOutputStream fstream = new FileOutputStream(new File(path));
			ObjectOutputStream oos = new ObjectOutputStream(fstream);
			oos.writeObject(page);
			oos.close();
		} catch (IOException e) {
			System.err.println("Error Writing to file");
			e.printStackTrace(System.err);
		}
	}
	public int getPageCount()
	{
		return this.pageCount;
	}
//	public String getPrimayKey()
//	{
//		return this.primaryKey;
//	}
	public void incPageCount()
	{
		this.pageCount++;
	}
}
