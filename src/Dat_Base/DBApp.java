package Dat_Base;

import java.io.IOException;
import java.util.Hashtable;

public class DBApp {

	private DBHelper dbHelper;
	private Hashtable<String, Table> tables;

	public DBApp() {
		init();
	}

	public void init() {
		this.dbHelper = new DBHelper();
		this.tables = new Hashtable<>();
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		if (tables.containsKey(strTableName))
			throw new DBAppException("Table %s is already created!");

		for (String type : htblColNameType.values())
			if(!dbHelper.isTypeSupported(type))
				throw new DBAppException("Un Supported Data Type " + type);

		Table newTable = new Table(dbHelper, strTableName);
		tables.put(strTableName, newTable);

		try {
			dbHelper.addToMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
		} catch (IOException e) {
			System.err.printf("Table %s cannot be created!\n", strTableName);
			System.err.printf("Error while writing to Metadata", strTableName);
			e.printStackTrace(System.err);
			tables.remove(strTableName);
		}
	}

	public DBHelper getDbHelper() {
		return dbHelper;
	}

	public static void main(String[] args) throws Exception {
//		DBApp db = new DBApp();
		DBHelper db = new DBHelper();

	}

}
