package Dat_Base;

import java.util.Hashtable;

public class DBAppTest {
	
	public static void main(String[] args) throws DBAppException {
		DBApp db = new DBApp();
		Hashtable<String, String> table1 = new Hashtable<>();
		table1.put("x", "java.lang.Integer");
		table1.put("y", "java.lang.String");
		table1.put("z", "java.lang.Double");
		
		db.createTable("table1", "y", table1);
		
		Hashtable<String, String> table2 = new Hashtable<>();
		table2.put("x", "java.util.Date");
		table2.put("y", "java.lang.String");
		table2.put("z", "java.lang.Double");
		
		db.createTable("table2", "y", table2);
		
		String strTableName = "Student";
		
		Hashtable<String, String> htblColNameType = new Hashtable<>( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		
		db.createTable( strTableName, "id", htblColNameType );
	}
	
}
