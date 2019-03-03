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
	}

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
}
