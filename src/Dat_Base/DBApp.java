package Dat_Base;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;

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
	
	public boolean insertIntoTableString (String strTableName,Hashtable<String,Object> htblColNameValue) throws Exception
	{
		Table table=tables.get(strTableName);
		if(table == null)
		{
			System.out.println("Table is not found!");
			return false;
		}
		FileReader fileReader = new FileReader("/data/metadata.csv");
		BufferedReader br = new BufferedReader(fileReader);
		String curLine="";
		ArrayList<HashMap<String, String>>tableCol=new ArrayList<>();
		String[] info = {"","name","type","key","indexed"};
		while((curLine=br.readLine())!=null)
		{
			
			//check the split
			String[]s=curLine.split(", ");
			if(!s[0].equals(strTableName))continue;
			
			HashMap<String, String>colInfo=new HashMap<>();
			for(int i=1;i<5;i++)
				colInfo.put(info[i], s[i]);
			tableCol.add(colInfo);
		}
		if(tableCol.isEmpty()) return false;
		
		for(Entry<String, Object>e:htblColNameValue.entrySet())
		{
			String colName=e.getKey();
			String value=(String)e.getValue();
			Object checkedValue;
			boolean found=false;
			for(HashMap<String, String>hm:tableCol)
			{
				if(hm.get("name").equals(colName))
				{
					String type=hm.get("type");
					checkedValue = dbHelper.reflect(type, value);
					if(checkedValue!=null)
					{
						found=true;
						break;
					}
				}		
			}
			if(!found)
				return false;
		}
		// Valid Data to be inserted
		// We have to insert in the right page
		int start=0;
		int end=table.getPageCount();
		boolean added=false;
		Page curPage=null;
		// trying to add the record sequentially
		while(start < end && !added )
		{
			curPage = table.readPage("./data/"+strTableName+"_"+start);
			added |= curPage.addRecord(htblColNameValue);
			start++;
		}
		
		if( !added ) // we could not insert in any of the original pages, we have to make new page
		{
			Page newPage = new Page(table.getPrimayKey());
			newPage.addRecord(htblColNameValue);
			table.writePage("./data/"+strTableName+"_"+end, newPage);
			table.incPageCount();
		}
		else // we have to check that the page we inserted in did not reach the limit
		{
			start--; // back to the curPage
			table.writePage("./data/"+strTableName+"_"+start, curPage);
			int size = curPage.getSize();
//			MaximumRowsCountInPage
			fileReader = new FileReader("/config/DBApp.config");
			br = new BufferedReader(fileReader);
			curLine=br.readLine();
			String s[]=curLine.split(" : ");
			int MaximumRowsCountInPage=Integer.parseInt(s[1]);
			while( size > MaximumRowsCountInPage ) //pushing the extra elments to the last empty page
			{
				Hashtable<String, Object> lastRecoed=curPage.getPage().remove(size-1);
				table.writePage("./data/"+strTableName+"_"+start, curPage);
				start++;
				if(start == end) // we need to creat ean extra page
				{
					Page newPage = new Page(table.getPrimayKey());
					newPage.addRecord(lastRecoed);
					table.writePage("./data/"+strTableName+"_"+end, newPage);
					table.incPageCount();
					break;
				}
				Page nextPage = table.readPage("./data/"+strTableName+"_"+start);
				nextPage.addRecord(lastRecoed);
				table.writePage("./data/"+strTableName+"_"+start, nextPage);
				size = nextPage.getSize();
				curPage=nextPage;
			}
		}
		// Write all Pages
		
		return true;
	}
	
	public void deleteFromTabledeleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue)
	{
		
	}
	
	public DBHelper getDbHelper() {
		return dbHelper;
	}
	
	public static void main(String[] args) throws Exception {
//		DBApp db = new DBApp();
		
	}

}
