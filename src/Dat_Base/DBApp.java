package Dat_Base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Map.Entry;

public class DBApp {

	private DBHelper dbHelper;
	private Hashtable<String, Table> tables;

	public DBApp() {
		init();
	}

	public void init() {
		this.dbHelper = new DBHelper();
		this.tables = dbHelper.getTables();
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

		Table newTable = new Table(strTableName);
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

	public boolean insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Table table = tables.get(strTableName);
		if (table == null)
			throw new DBAppException("Table is not found!");

		FileReader fileReader = null;
		try {
			fileReader = new FileReader(dbHelper.getDBPath() + "/data/metadata.csv");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		BufferedReader br = new BufferedReader(fileReader);
		String curLine = "";
		ArrayList<HashMap<String, String>> tableCol = new ArrayList<>();
		String[] info = { "", "name", "type", "key", "indexed" };
		String primaryKey = "";

		try {
			while ((curLine = br.readLine()) != null) {
				// check the split
				String[] s = curLine.split(",");
				if (!s[0].equals(strTableName))
					continue;

				HashMap<String, String> colInfo = new HashMap<>();
				for (int i = 1; i < 5; i++)
					colInfo.put(info[i], s[i]);

				if (colInfo.get("key").equals("True"))
					primaryKey = s[1];

				tableCol.add(colInfo);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		if (tableCol.isEmpty())
			throw new DBAppException("Table is not found!");

		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			String colName = e.getKey();
			String value = e.getValue() + "";
			Object checkedValue = null;
			boolean found = false;
			for (HashMap<String, String> hm : tableCol) {
				if (hm.get("name").equals(colName)) {
					String type = hm.get("type");
					try {
						checkedValue = dbHelper.reflect(type, value);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					if (checkedValue != null) {
						found = true;
						break;
					}
				}
			}
			if (!found)
				throw new DBAppException("Column " + colName + " does not exist!");
		}
		// Valid Data to be inserted
		// We have to insert in the right page
		htblColNameValue.put("TouchDate", dbHelper.currentDate());
		Record record = new Record(primaryKey, htblColNameValue);
		
		int start = 0;
		int end = table.getPageCount();
		boolean added = false;
		Page curPage = null;
		// trying to add the record sequentially
		while (start < end && !added) {
			curPage = table.readPage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start);
			added |= curPage.addRecord(record, dbHelper.getMaximumRowsCountInPage());
			start++;
		}

		if (!added) { // we could not insert in any of the original pages, we have to make new page
			Page newPage = new Page();
			newPage.addRecord(record, dbHelper.getMaximumRowsCountInPage());
			table.writePage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + end, newPage);
			table.incPageCount();
		} else { // we have to check that the page we inserted in did not reach the limit
			start--; // back to the curPage
			table.writePage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start, curPage);
			int size = curPage.getSize();
//			MaximumRowsCountInPage
			int MaximumRowsCountInPage = dbHelper.getMaximumRowsCountInPage();
			while (size > MaximumRowsCountInPage) { // pushing the extra elements to the last empty page
				Record lastRecoed = curPage.getPage().remove(size - 1);
				table.writePage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start,
						curPage);
				start++;
				if (start == end) { // we need to create an extra page
					Page newPage = new Page();
					newPage.addRecord(lastRecoed, dbHelper.getMaximumRowsCountInPage());
					table.writePage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + end,
							newPage);
					table.incPageCount();
					break;
				}
				Page nextPage = table
						.readPage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start);
				nextPage.addRecord(lastRecoed, dbHelper.getMaximumRowsCountInPage());
				table.writePage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start,
						nextPage);
				size = nextPage.getSize();
				curPage = nextPage;
			}
		}
		return true;
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Table table = tables.get(strTableName);
		if (table == null)
			throw new DBAppException("Table is not found!");

		Hashtable<String, Object> colTableInfo = null;
		try {
			colTableInfo = dbHelper.getTableColNameType(strTableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			String colName = e.getKey();
			if (!colTableInfo.containsKey(colName))
				throw new DBAppException("Column ( " + colName + " ) does not exist");
			Object value = e.getValue();
			Object type = colTableInfo.get(colName);
			if (!value.getClass().equals(type.getClass()))
				throw new DBAppException("The type of ( " + value + " ) is not right, must be " + type);
		}
		// all types are now right

		int start_read = 0, start_write = 0;
		int end_read = table.getPageCount(), end_write = table.getPageCount();
		Page writePage = new Page();
		while (start_read < end_read) {
			Page curPage = table
					.readPage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start_read++);

			Vector<Record> v = curPage.getPage();
			for (int i = 0; i < curPage.getSize(); i++) {
				if (!dbHelper.matchRecord(v.get(i).getRecord(), htblColNameValue)) {
					writePage.addRecord(v.get(i), dbHelper.getMaximumRowsCountInPage());
					if (writePage.getSize() == dbHelper.getMaximumRowsCountInPage()) {
						table.writePage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_"
								+ start_write++, writePage);
						writePage = new Page();
					}
				}

			}
		}
		if (writePage.getSize() > 0)
			table.writePage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start_write++,
					writePage);

		table.setPageCount(start_write);

		while (start_write < end_write) { // delete extra pages
			File file = new File(
					dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start_write++);
			file.delete();
		}
	}

	public DBHelper getDbHelper() {
		return dbHelper;
	}

	public Hashtable<String, Table> getTables() {
		return tables;
	}
}
