package Dat_Base;

import java.io.IOException;
import java.util.Hashtable;

public class DBAppTest {

	public static void testCreation(DBApp db) throws DBAppException {
		String strTableName = "Student";
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		db.createTable(strTableName, "id", htblColNameType);
		System.out.println("###############");
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		
		db.createBitmapIndex(strTableName, "id");
		db.createBitmapIndex(strTableName, "gpa");

		htblColNameValue.put("id", new Integer(2343432));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.95));
		db.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		
		System.out.println("-----------------");
		printIndexPage(db, "Student", "id", 0);
		System.out.println("-----------------");

		htblColNameValue.put("id", new Integer(2343223));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.95));
		db.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();

//		System.out.println("-----------------");
//		printIndexPage(db, "Student", "id", 0);
//		System.out.println("-----------------");
		
		htblColNameValue.put("id", new Integer(5674567));
		htblColNameValue.put("name", new String("Dalia Noor"));
		htblColNameValue.put("gpa", new Double(1.25));
		db.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();

//		System.out.println("-----------------");
//		printIndexPage(db, "Student", "id", 0);
//		System.out.println("-----------------");
		
		System.out.println(db.getTables().get(strTableName).getPageCount());
		htblColNameValue.put("id", new Integer(23498));
		htblColNameValue.put("name", new String("John Noor"));
		htblColNameValue.put("gpa", new Double(1.5));
		db.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println(db.getTables().get(strTableName).getPageCount());

		htblColNameValue.put("id", new Integer(78452));
		htblColNameValue.put("name", new String("Zaky Noor"));
		htblColNameValue.put("gpa", new Double(0.88));
		db.insertIntoTable(strTableName, htblColNameValue);
		System.out.println(db.getTables().get(strTableName).getPageCount());
		System.out.println("###############");
		
//		htblColNameValue.clear();
//		htblColNameValue.put("name", "Zaky Noor");
//		
//		db.deleteFromTable(strTableName, htblColNameValue);
		
		htblColNameValue.clear();
		htblColNameValue.put("gpa", 0.69);
		
		db.updateTable(strTableName, "78452", htblColNameValue);
		
//		db.createBitmapIndex(strTableName, "id");
//		db.createBitmapIndex(strTableName, "gpa");

	}

	public static void printPage(DBApp db, String tableName, int pageNumber) {
		String path = db.getDbHelper().getDBPath() + "/data/" + tableName + "/" + tableName + "_"
				+ String.valueOf(pageNumber);
		Page p = new Table(tableName).readPage(path);
		System.out.println(p);
	}

	public static void printIndexPage(DBApp db, String tableName, String colName, int pageNumber) {
		String path = db.getDbHelper().getIndexPagePath(tableName, colName, pageNumber);
		IndexPage p = new BitMap(tableName, colName, db.getDbHelper()).readPage(path);
		System.out.println(p);
	}

	public static void main(String[] args) throws DBAppException, IOException {
		DBApp db = new DBApp();

		testCreation(db);

		printPage(db, "Student", 0);
		printIndexPage(db, "Student", "id", 0);
		printIndexPage(db, "Student", "gpa", 0);

//		System.out.println(db.getTables());
//		Hashtable<String, String> table1 = new Hashtable<>();
//		table1.put("x", "java.lang.Integer");
//		table1.put("y", "java.lang.String");
//		table1.put("z", "java.lang.Double");
//
//		db.createTable("table1", "y", table1);
//
//		Hashtable<String, String> table2 = new Hashtable<>();
//		table2.put("x", "java.util.Date");
//		table2.put("y", "java.lang.String");
//		table2.put("z", "java.lang.Double");
//
//		db.createTable("table2", "y", table2);

//		String strTableName = "Student";
//
//		Hashtable<String, String> htblColNameType = new Hashtable<>();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//
//		db.createTable(strTableName, "id", htblColNameType);
//		ArrayList<Integer> a = new ArrayList<>();
//		for (int j = 45; j > -1; j--) {
//			int i = j;
//			Hashtable<String, Object> in = new Hashtable<>();
//			in.put("id", i);
//			in.put("name", "Manta_" + i);
//			in.put("gpa", 1.3);
//
//			try {
//				db.insertIntoTable(strTableName, in);
//				a.add(i);
//			} catch (Exception e) {
//				System.out.println(e.getMessage());
//				e.printStackTrace(System.err);
//			}
//		}
//		int i = 0;
//		while (i < 3)
//			System.out.println(db.getTables().get(strTableName)
//					.readPage(db.getDbHelper().getDBPath() + "data/" + strTableName + "/" + strTableName + "_" + i++));
//		Hashtable<String, Object> in = new Hashtable<>();
//		in.put("name", "ouda");
//		in.put("id", 46);
//		db.updateTable(strTableName, "5", in);
//		i=0;
//		while (i < 3)
//			System.out.println(db.getTables().get(strTableName)
//					.readPage(db.getDbHelper().getDBPath() + "data/" + strTableName + "/" + strTableName + "_" + i++));

//		System.out.println("-------------");
//
//		for (i = 20; i > -1; i--) {
//			Hashtable<String, Object> out = new Hashtable<>();
//			out.put("id", i);
//			db.deleteFromTable(strTableName, out);
//		}
//		i = 0;
//		while (i < 3)
//			System.out.println(db.getTables().get(strTableName)
//					.readPage(db.getDbHelper().getDBPath() + "data/" + strTableName + "/" + strTableName + "_" + i++));

	}

}
