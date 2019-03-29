package Dat_Base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class UnitTest {

	static String[]names= {"Yahia","Ouda","Ronic","Manta","Merna","Youstina","Joe","Ziad","Moe"};
	public static void testUpdate(DBApp db) throws DBAppException{
		System.out.println("----------- Testing Update -----------");
//		String strTableName = "Student";
		double gpaold = (double)((int)(Math.random()*10));
		double gpanew = (double)((int)(Math.random()*10));
		System.out.println("We will update the GPA from: "+gpaold+" to GPA: "+ gpanew);
		System.out.println("----------- End Of Testing Updates -----------");
	}
	public static void testBitMap(DBApp db) throws DBAppException{
		System.out.println("----------- Testing BitMap -----------");
		String strTableName = "Student";
		
		db.createBitmapIndex(strTableName, "gpa");
		System.out.println("BitMap Index Created on Column GPA");
		double gpa = (double)((int)(Math.random()*10));
		System.out.println("Searching for GPA: "+gpa);
		ArrayList<Integer>rows = getRows(db, strTableName, "gpa", new Double(gpa));
		System.out.println("They appear in rows: \n"+rows);
		System.out.println("\n     ------ BitMap Table of GPA ------     ");
		printIndexTable(db, "Student", "gpa");
		System.out.println("     ------ End Of Table ------     \n");
		
		db.createBitmapIndex(strTableName, "name");
		System.out.println("BitMap Index Created on Column Name");
		int name = (int)(Math.random()*9);
		System.out.println("Searching for Name: "+names[name]);
		rows = getRows(db, strTableName, "name", new String(names[name]));
		System.out.println("They appear in rows: \n"+rows);
		System.out.println("\n     ------ BitMap Table of Name ------     ");
		printIndexTable(db, "Student", "name");
		System.out.println("     ------ End Of Table ------     \n");
		System.out.println("----------- End Of Testing BitMap -----------");
	}
	public static void testInsertion(DBApp db) throws DBAppException{
		System.out.println("----------- Testing Insertions -----------");
		String strTableName = "Student";
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		
		System.out.println("Inserting From Top:");
		for(int i=1;i<=5;i++)
		{
			int name = (int)(Math.random()*9);
			double gpa = (double)((int)(Math.random()*10));
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String(names[name]));
			htblColNameValue.put("gpa", new Double(gpa));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
		}
		System.out.println("Done inserting 5 records from id 1 to 5");
		System.out.println("Number of pages: "+db.getTables().get(strTableName).getPageCount());
		System.out.println("     ------ Table ------     ");
		printTable(db, "Student");
		System.out.println("     ------ End Of Table ------     ");
		System.out.println("\nInserting From Bottom:");
		for(int i=15;i>10;i--)
		{
			int name = (int)(Math.random()*9);
			double gpa = (double)((int)(Math.random()*10));
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String(names[name]));
			htblColNameValue.put("gpa", new Double(gpa));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
		}
		System.out.println("Done inserting 5 records from id 15 to 11");
		System.out.println("Number of pages: "+db.getTables().get(strTableName).getPageCount());
		System.out.println("     ------ Table ------     \n");
		printTable(db, "Student");
		System.out.println("\n     ------ End Of Table ------     ");
		System.out.println("\nInserting Inside:");
		for(int i=10;i>5;i--)
		{
			int name = (int)(Math.random()*9);
			double gpa = (double)((int)(Math.random()*10));
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String(names[name]));
			htblColNameValue.put("gpa", new Double(gpa));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
		}
		System.out.println("Done inserting 5 records from id 10 to 6");
		System.out.println("Number of pages: "+db.getTables().get(strTableName).getPageCount());
		System.out.println("     ------ Table ------     \n");
		printTable(db, "Student");
		System.out.println("\n     ------ End Of Table ------     ");
		String seq="";
		for(int i=0;i<20;i++)
		{
			int name = (int)(Math.random()*9);
			double gpa = (double)((int)(Math.random()*10));
			int x = (int)(Math.random()*40)+16;
			seq += x+" ";
			htblColNameValue.put("id", new Integer(x));
			htblColNameValue.put("name", new String(names[name]));
			htblColNameValue.put("gpa", new Double(gpa));
			try {
			db.insertIntoTable(strTableName, htblColNameValue);
			}
			catch(Exception e){
				i--;
			}
			htblColNameValue.clear();
		}
		System.out.println("Done inserting 20 Random records seq = "+seq);
		System.out.println("Number of pages: "+db.getTables().get(strTableName).getPageCount());
		System.out.println("     ------ Table ------     \n");
		printTable(db, "Student");
		System.out.println("\n     ------ End Of Table ------     ");
		System.out.println("----------- End Of Insertions -----------\n");

	}
	public static void printIndexTable(DBApp db, String tableName, String colName)
	{
		BitMap bm = new BitMap(tableName, colName, db.getDbHelper(), db.getQueryManager());
		for(int i=0;i < bm.getPageCount();i++)
			printIndexPage(db, tableName, colName, i);
	}
	public static void printTable(DBApp db, String tableName)
	{
		for(int i=0;i<db.getTables().get(tableName).getPageCount();i++)
			printPage(db, tableName, i);
	}
	public static void testCreation(DBApp db) throws DBAppException {
		String strTableName = "Student";
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		db.createTable(strTableName, "id", htblColNameType);
		System.out.println("----------- Table Created Successfully!-----------");
		System.out.println("Number of pages: "+db.getTables().get(strTableName).getPageCount());
		System.out.println("Maximum record per pages: "+db.getDbHelper().getMaximumRowsCountInPage());
		System.out.println("--------------------------------------------------");
		

	}
	
	@SuppressWarnings("unchecked")
	public static ArrayList<Integer> getRows(DBApp db, String tableName, String colName, Object o)
	{
		Comparable<Object> value = (Comparable<Object>)o;
		ArrayList<Integer>a = new ArrayList<>();
		Table table = db.getTables().get(tableName);
		int pages = table.getPageCount();
		for(int i=0; i < pages; i++)
		{
			Page p = table.readPage(db.getDbHelper().getPagePath(tableName, i));
			ArrayList<Integer> tmp = getRow(p, colName, value);
			int offset = i*db.getDbHelper().getMaximumRowsCountInPage();
			for(int x:tmp)
			{
				a.add( offset + x );
			}
		}
		return a;
	}
	
	@SuppressWarnings("unchecked")
	public static ArrayList<Integer> getRow(Page p, String colName, Comparable<Object> value)
	{
		Vector<Record> v = p.getPage();
		ArrayList<Integer> a = new ArrayList<>();
		int i=0;
		for(Record r:v)
		{
			if(((Comparable<Object>)r.getCell(colName)).compareTo(value) == 0)
			{
				a.add(i);
			}
			i++;
		}
		return a;
	}
	
	@SuppressWarnings("unchecked")
	public static boolean isInThisBitMap(DBApp db, String tableName, String colName, BitMap bp, Object o, ArrayList<Integer> row)
	{
		Comparable<Object> value = (Comparable<Object>)o;
		int pages = bp.getPageCount();
		for(int i=0;i < pages ;i++)
		{
			IndexPage p = getIndexPage(db, tableName, colName, i);
			if(isInThisPage(p, value, row))
				return true;
		}
		return false;
	}
	public static boolean isInThisPage(IndexPage p, Comparable<Object> value, ArrayList<Integer> row)
	{
		Vector<IndexPair>v=p.getIndexPage();
		for(IndexPair i:v)
		{
			if(i.getValue().compareTo(value) == 0)
			{
				return isInThisPair(i, value, row);
			}
		}
		return false;
	}
	public static boolean isInThisPair(IndexPair p, Comparable<Object> value, ArrayList<Integer> row)
	{
		String bits = p.getBits();
		boolean flag = true;
		for(int i:row)
			flag &= (bits.charAt(i) == '1');
		return flag;
	}
	public static void printPage(DBApp db, String tableName, int pageNumber) {
		String path = db.getDbHelper().getDBPath() + "/data/" + tableName + "/" + tableName + "_"
				+ String.valueOf(pageNumber);
		Page p = new Table(tableName, db.getDbHelper()).readPage(path);
		System.out.println(p);
	}
	public static Page getPage(DBApp db, String tableName, int pageNumber) {
		String path = db.getDbHelper().getDBPath() + "/data/" + tableName + "/" + tableName + "_"
				+ String.valueOf(pageNumber);
		Page p = new Table(tableName, db.getDbHelper()).readPage(path);
		return p;
	}

	public static void printIndexPage(DBApp db, String tableName, String colName, int pageNumber) {
		String path = db.getDbHelper().getIndexPagePath(tableName, colName, pageNumber);
		IndexPage p = new BitMap(tableName, colName, db.getDbHelper(), db.getQueryManager()).readPage(path);
		System.out.println(p);
	}
	public static IndexPage getIndexPage(DBApp db, String tableName, String colName, int pageNumber) {
		String path = db.getDbHelper().getIndexPagePath(tableName, colName, pageNumber);
		IndexPage p = new BitMap(tableName, colName, db.getDbHelper(), db.getQueryManager()).readPage(path);
		return p;
	}

	public static void main(String[] args) throws DBAppException, IOException {
		DBApp db = new DBApp();

		testCreation(db);
		testInsertion(db);
		testBitMap(db);
//		printPage(db, "Student", 0);
//		printIndexPage(db, "Student", "id", 0);
//		printIndexPage(db, "Student", "gpa", 0);
		
//		BitMap id = new BitMap("Student", "id",db.getDbHelper());
//		BitMap gpa = new BitMap("Student", "gpa", db.getDbHelper());
//		IndexPage p = getIndexPage(db, "Student", "id", 0);
		
//		ArrayList<Integer> v05=getRows(db, "Student", "gpa", new Double(0.5));
//		boolean f = isInThisBitMap(db, "Student", "gpa", gpa, new Double(0.5), v05);
//		System.out.println(f);
	}

}
