package Dat_Base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class UnitTest {

	public static void testCreation(DBApp db) throws DBAppException {
		String strTableName = "Student";
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		db.createTable(strTableName, "id", htblColNameType);
		System.out.println("###############");
		System.out.println(db.getTables().get(strTableName).getPageCount());
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();

		for(int i=0;i<50;i++)
		{
		htblColNameValue.put("id", new Integer(2343432+i));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.5));
		db.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		}
		System.out.println(db.getTables().get(strTableName).getPageCount());

		htblColNameValue.put("id", new Integer(2343));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.6));
		db.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println(db.getTables().get(strTableName).getPageCount());

		htblColNameValue.put("id", new Integer(5674567));
		htblColNameValue.put("name", new String("Dalia Noor"));
		htblColNameValue.put("gpa", new Double(0.5));
		db.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();

		System.out.println(db.getTables().get(strTableName).getPageCount());
		htblColNameValue.put("id", new Integer(23498));
		htblColNameValue.put("name", new String("John Noor"));
		htblColNameValue.put("gpa", new Double(0.5));
		db.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println(db.getTables().get(strTableName).getPageCount());

		htblColNameValue.put("id", new Integer(78452));
		htblColNameValue.put("name", new String("Zaky Noor"));
		htblColNameValue.put("gpa", new Double(0.5));
		db.insertIntoTable(strTableName, htblColNameValue);
		System.out.println(db.getTables().get(strTableName).getPageCount());
		System.out.println("###############");

		db.createBitmapIndex(strTableName, "id");
		db.createBitmapIndex(strTableName, "gpa");
		

	}
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
		Page p = new Table(tableName).readPage(path);
		System.out.println(p);
	}
	public static Page getPage(DBApp db, String tableName, int pageNumber) {
		String path = db.getDbHelper().getDBPath() + "/data/" + tableName + "/" + tableName + "_"
				+ String.valueOf(pageNumber);
		Page p = new Table(tableName).readPage(path);
		return p;
	}

	public static void printIndexPage(DBApp db, String tableName, String colName, int pageNumber) {
		String path = db.getDbHelper().getIndexPagePath(tableName, colName, pageNumber);
		IndexPage p = new BitMap(tableName, colName).readPage(path);
		System.out.println(p);
	}
	public static IndexPage getIndexPage(DBApp db, String tableName, String colName, int pageNumber) {
		String path = db.getDbHelper().getIndexPagePath(tableName, colName, pageNumber);
		IndexPage p = new BitMap(tableName, colName).readPage(path);
		return p;
	}

	public static void main(String[] args) throws DBAppException, IOException {
		DBApp db = new DBApp();

		testCreation(db);

		printPage(db, "Student", 0);
		printIndexPage(db, "Student", "id", 0);
		printIndexPage(db, "Student", "gpa", 0);
		
		BitMap id = new BitMap("Student", "id");
		BitMap gpa = new BitMap("Student", "gpa");
		IndexPage p = getIndexPage(db, "Student", "id", 0);
		
		ArrayList<Integer> v05=getRows(db, "Student", "gpa", new Double(0.5));
		boolean f = isInThisBitMap(db, "Student", "gpa", gpa, new Double(0.5), v05);
		System.out.println(f);
	}

}
