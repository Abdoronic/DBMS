package Dat_Base;

import java.util.Hashtable;
import java.util.Vector;

public class DBApp {

	private DBHelper dbHelper;

	public DBApp() {
		init();
	}

	public void init() {
		this.dbHelper = new DBHelper();
	}

	public DBHelper getDbHelper() {
		return dbHelper;
	}
	
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType)  {
		new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		addToMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
	}
	
	public void addToMetaData(String tableName, String primaryKey, Hashtable<String, String> colNameType) {
		
	}
	
	public static void main(String[] args) throws Exception {
//		DBApp db = new DBApp();
		Vector<Integer> v = new Vector<>();
		v.add(1);
		v.add(2);
		v.add(3);
		v.add(4);
		v.add(2, 5);
		System.out.println(v);
		
	}

}
