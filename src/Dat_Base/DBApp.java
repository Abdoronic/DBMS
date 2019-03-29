package Dat_Base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

public class DBApp {

	private DBHelper dbHelper;
	private QueryManager queryManager;
	private Hashtable<String, Table> tables;

	public DBApp() {
		init();
	}

	public void init() {
		this.dbHelper = new DBHelper();
		this.queryManager = new QueryManager(dbHelper);
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

		Table newTable = new Table(strTableName, dbHelper);
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

	@SuppressWarnings("unchecked")
	public void createBitmapIndex(String strTableName, String strColName) throws DBAppException {
		// Check if Input is valid
		if (!tables.containsKey(strTableName))
			throw new DBAppException("Table " + strTableName + " Does not exist!");
		if (dbHelper.isIndexed(strTableName, strTableName))
			throw new DBAppException("Index on " + strColName + " in" + strTableName + " is already created!");
		try {
			Hashtable<String, Object> colNameType = dbHelper.getTableColNameType(strTableName);
			if (!colNameType.containsKey(strColName))
				throw new DBAppException("Column " + strColName + " Does not exist in Table " + strTableName);
		} catch (IOException e) {
			System.err.println("Error Getting ColName-Type of" + strTableName + " from metadata");
			e.printStackTrace(System.err);
		}
		// Create required Folders
		BitMap bitMap = new BitMap(strTableName, strColName, dbHelper, queryManager);
		// Write to metadata
		dbHelper.setIndexed(strTableName, strColName);

		Table table = dbHelper.getTable(strTableName);

		// Get Total number of records in Table
		int tableSize = dbHelper.calcTableSize(table);

		// Create a Frequency for all the unique values
		TreeMap<Comparable<Object>, IndexPair> frequency = new TreeMap<>();
		int recordIndex = 0;
		for (int i = 0; i < table.getPageCount(); i++) {
			Page page = table.readPage(dbHelper.getPagePath(strTableName, i));
			for (Record r : page.getPage()) {
				Comparable<Object> value = (Comparable<Object>) r.getCell(strColName);
				if (frequency.containsKey(value)) {
					frequency.get(value).set(recordIndex);
				} else {
					IndexPair pair = new IndexPair(value, tableSize);
					pair.set(recordIndex);
					frequency.put(value, pair);
				}
				recordIndex++;
			}
		}
		// Write the Index pages
		int maxPairsPerPage = dbHelper.getBitmapSize();
		int i = 0, pc = 0;
		Vector<IndexPair> indexPairBucket = null;
		for (Map.Entry<Comparable<Object>, IndexPair> entry : frequency.entrySet()) {
			indexPairBucket = (i == 0) ? new Vector<>() : indexPairBucket;
			indexPairBucket.add(entry.getValue());
			System.out.println(entry.getValue());
			if (++i > maxPairsPerPage) {
				i = 0;
				bitMap.writePage(dbHelper.getIndexPagePath(strTableName, strColName, pc++),
						new IndexPage(indexPairBucket));
			}
		}
		if (i > 0)
			bitMap.writePage(dbHelper.getIndexPagePath(strTableName, strColName, pc), new IndexPage(indexPairBucket));
		bitMap.setPageCount(pc);
	}

	@SuppressWarnings("unchecked")
	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Table table = dbHelper.getTable(strTableName);
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
		// ------------------------------------------------------valid data
		int start_read = 0;
		int end_read = table.getPageCount();
		int counter = 0, insertedIndex = 0;
		boolean keychanged = false, newIdBigger = false;

		Hashtable<String, Object> tmp = new Hashtable<String, Object>();
		while (start_read < end_read) {
			Page curPage = table.readPage(dbHelper.getPagePath(strTableName, start_read++));
			Vector<Record> v = curPage.getPage();
			for (int i = 0; i < curPage.getSize(); i++, insertedIndex++) {
				newIdBigger = false;
				if ((v.get(i).getPrimaryKey() + "").equals(strKey)) {
					counter++;
					Hashtable<String, Object> old = v.get(i).getRecord();
					for (Entry<String, Object> e : htblColNameValue.entrySet()) {
						String colName = e.getKey();
						Object value = e.getValue();
						if (colName.equals(v.get(i).getPrimaryKeyCN())) {
							if (!strKey.equals(value)) {
								keychanged = true;
								newIdBigger = ((Comparable<Object>) value)
										.compareTo((Comparable<Object>) htblColNameValue.get(colName)) <= 0;
								tmp.put(v.get(i).getPrimaryKeyCN(), old.get(v.get(i).getPrimaryKeyCN()));
							}
						}
						if (old.containsKey(colName)) {
							if (dbHelper.isIndexed(strTableName, colName)) {
								BitMap colBitMap = new BitMap(strTableName, colName, dbHelper, queryManager);
								colBitMap.updateBitMap((Comparable<Object>) old.get(colName),
										(Comparable<Object>) htblColNameValue.get(colName), insertedIndex);
							}
							old.put(colName, htblColNameValue.get(colName));
						}

						old.put("TouchDate", dbHelper.currentDate());
					}
					if (keychanged) {
						keychanged = false;
						old.remove("TouchDate");
						deleteFromTable(strTableName, tmp);
						insertIntoTable(strTableName, old);
						if (newIdBigger)
							--insertedIndex;
					} else {
						table.writePage(dbHelper.getPagePath(strTableName, start_read - 1), curPage);
					}

				}
			}

		}
		if (counter == 0) {
			throw new DBAppException("There is no entry with this key");
		}

	}

	@SuppressWarnings("unchecked")
	public boolean insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Table table = dbHelper.getTable(strTableName);
		if (table == null)
			throw new DBAppException("Table is not found!");

		FileReader fileReader = null;
		try {
			fileReader = new FileReader(dbHelper.getDBPath() + "/data/metadata.csv");
		} catch (FileNotFoundException e) {
			System.err.println("Error Reading metadata");
			e.printStackTrace();
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
		int recordTableIndex = 0, recordPageIndex;
		int maximumRowsCountInPage = dbHelper.getMaximumRowsCountInPage();
		boolean added = false;
		Page curPage = null;
		// trying to add the record sequentially
		while (start < end && !added) {
			curPage = table.readPage(dbHelper.getPagePath(strTableName, start));
			recordPageIndex = curPage.addRecord(record, maximumRowsCountInPage);
			if (recordPageIndex == -1) {
				recordTableIndex += maximumRowsCountInPage;
			} else {
				recordTableIndex += recordPageIndex;
				added = true;
			}
			start++;
		}
		if (!added) { // we could not insert in any of the original pages, we have to make new page
			Page newPage = new Page();
			newPage.addRecord(record, dbHelper.getMaximumRowsCountInPage());
			table.writePage(dbHelper.getPagePath(strTableName, end), newPage);
			table.incPageCount();
		} else { // we have to check that the page we inserted in did not reach the limit
			start--; // back to the curPage
			table.writePage(dbHelper.getPagePath(strTableName, start), curPage);
			int size = curPage.getSize();
//			MaximumRowsCountInPage
			int MaximumRowsCountInPage = dbHelper.getMaximumRowsCountInPage();
			while (size > MaximumRowsCountInPage) { // pushing the extra elements to the last empty page
				Record lastRecoed = curPage.getPage().remove(size - 1);
				table.writePage(dbHelper.getPagePath(strTableName, start), curPage);
				start++;
				if (start == end) { // we need to create an extra page
					Page newPage = new Page();
					newPage.addRecord(lastRecoed, dbHelper.getMaximumRowsCountInPage());
					table.writePage(dbHelper.getPagePath(strTableName, end), newPage);
					table.incPageCount();
					break;
				}
				Page nextPage = table.readPage(dbHelper.getPagePath(strTableName, start));
				nextPage.addRecord(lastRecoed, dbHelper.getMaximumRowsCountInPage());
				table.writePage(dbHelper.getPagePath(strTableName, start), nextPage);
				size = nextPage.getSize();
				curPage = nextPage;
			}
		}
		for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
			if (dbHelper.isIndexed(strTableName, e.getKey())) {
				BitMap colBitMap = new BitMap(strTableName, e.getKey(), dbHelper, queryManager);
				colBitMap.insertIntoBitMap((Comparable<Object>) e.getValue(), recordTableIndex, true);
			}
		}
		return true;
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Table table = dbHelper.getTable(strTableName);
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
		int recordIndex = 0, deleted = 0;
		while (start_read < end_read) {
			Page curPage = table.readPage(dbHelper.getPagePath(strTableName, start_read++));
			Vector<Record> v = curPage.getPage();
			for (int i = 0; i < curPage.getSize(); i++, recordIndex++) {
				if (!dbHelper.matchRecord(v.get(i).getRecord(), htblColNameValue)) {
					writePage.addRecord(v.get(i), dbHelper.getMaximumRowsCountInPage());
					if (writePage.getSize() == dbHelper.getMaximumRowsCountInPage()) {
						table.writePage(dbHelper.getPagePath(strTableName, start_write++), writePage);
						writePage = new Page();
					}
				} else {
					for (String colName : v.get(i).getRecord().keySet()) {
						if (dbHelper.isIndexed(strTableName, colName)) {
							BitMap colBitMap = new BitMap(strTableName, colName, dbHelper, queryManager);
							colBitMap.deleteFromBitMap(recordIndex - deleted);
						}
					}
					deleted++;
				}
			}
		}
		if (writePage.getSize() > 0)
			table.writePage(dbHelper.getPagePath(strTableName, start_write++), writePage);

		table.setPageCount(start_write);

		while (start_write < end_write) { // delete extra pages
			File file = new File(dbHelper.getPagePath(strTableName, start_write++));
			file.delete();
		}
	}

	public Iterator<Record> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		if (arrSQLTerms.length == 0)
			throw new DBAppException("Invalid Where Clause");
		String tableName = arrSQLTerms[0].getTableName();
		if(!tables.containsKey(tableName))
			throw new DBAppException("Table Does not exist");
		Hashtable<String, Object> htbColNameValue = null;
		try {
			htbColNameValue = dbHelper.getTableColNameType(tableName);
		} catch (IOException e) {
			System.err.println("Error Reading From Metadata");
			e.printStackTrace();
			return null;
		}
		for (int i = 0; i < arrSQLTerms.length; i++) {
			if (!arrSQLTerms[i].getTableName().equals(tableName))
				throw new DBAppException("Select must be from the same Table");
			if (!htbColNameValue.containsKey(arrSQLTerms[i].getColumnName()))
				throw new DBAppException(
						"Column: " + arrSQLTerms[i].getColumnName() + " does not exist in Table: " + tableName);
		}
		Vector<Record> result = new Vector<>();
		
		Table table = dbHelper.getTable(tableName);
		
		if(table.getPageCount() == 0) return result.iterator();
		
		QueryManager queryManager = new QueryManager(dbHelper);
		
		String bits = queryManager.getSearchSpace(arrSQLTerms, strarrOperators);
		
		int maximumRowsCountInPage = dbHelper.getMaximumRowsCountInPage();
		
		int pc = 0;
		Page currPage = table.readPage(dbHelper.getPagePath(tableName, pc));
		for(int i = 0; i < bits.length(); i++) {
			if(bits.charAt(i) == '0') continue;
			int recordPage = i / maximumRowsCountInPage;
			int recordIndex = i % maximumRowsCountInPage;
			if(recordPage > pc)
				currPage = table.readPage(dbHelper.getPagePath(tableName, ++pc));
			if(queryManager.verfiyWhereClause(arrSQLTerms, strarrOperators, currPage.getRecord(recordIndex)))
				result.add(currPage.getRecord(recordIndex));
		}
		return result.iterator();
	}

	public DBHelper getDbHelper() {
		return dbHelper;
	}
	
	public QueryManager getQueryManager() {
		return queryManager;
	}

	public Hashtable<String, Table> getTables() {
		return tables;
	}
}
