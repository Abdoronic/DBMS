package Dat_Base;

//<<<<<<< HEAD
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
//=======
import java.io.IOException;
//>>>>>>> 086a7631be2aac84f306923a58b3b4599d7141cd
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

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		if (tables.containsKey(strTableName))
			throw new DBAppException("Table " + strTableName + " is already created!");

		for (String type : htblColNameType.values())
			if (!dbHelper.isTypeSupported(type))
				throw new DBAppException("Un Supported Data Type " + type);

		if (!htblColNameType.containsKey(strClusteringKeyColumn))
			throw new DBAppException("Table " + strTableName + " must have a primary key!");

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
//<<<<<<< HEAD
	
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
		String primaryKey = "";
		while((curLine=br.readLine())!=null)
		{
			
			//check the split
			String[]s=curLine.split(", ");
			if(!s[0].equals(strTableName))continue;
			
			HashMap<String, String>colInfo=new HashMap<>();
			for(int i=1;i<5;i++)
				colInfo.put(info[i], s[i]);

			if(colInfo.get("key").equals("true"))
				primaryKey = s[1];
			
			tableCol.add(colInfo);
		}
		if(tableCol.isEmpty()) return false;
		
		Record record=new Record(primaryKey, htblColNameValue);
		
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
			added |= curPage.addRecord(record);
			start++;
		}
		
		if( !added ) // we could not insert in any of the original pages, we have to make new page
		{
			Page newPage = new Page();
			newPage.addRecord(record);
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
				Record lastRecoed=curPage.getPage().remove(size-1);
				table.writePage("./data/"+strTableName+"_"+start, curPage);
				start++;
				if(start == end) // we need to creat ean extra page
				{
					Page newPage = new Page();
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
	
//=======
//
//>>>>>>> 086a7631be2aac84f306923a58b3b4599d7141cd
	public DBHelper getDbHelper() {
		return dbHelper;
	}

}
