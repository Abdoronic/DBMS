package Dat_Base;

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
	
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType)  {
		Table t = new Table(dbHelper, strTableName, strClusteringKeyColumn, htblColNameType);
		tables.put(strTableName, t);
		addToMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
	}
	
	public void addToMetaData(String tableName, String primaryKey, Hashtable<String, String> colNameType) {
		for(String colName : colNameType.keySet()) {
			
		}
	}
	
	public DBHelper getDbHelper() {
		return dbHelper;
	}
	
	public static void main(String[] args) throws Exception {
//		DBApp db = new DBApp();
		
	}

}
