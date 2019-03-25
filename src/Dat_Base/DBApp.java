package Dat_Base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;
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
		BitMap bitMap = new BitMap(strTableName, strColName);
		// Write to metadata
		dbHelper.setIndexed(strTableName, strColName);

		Table table = tables.get(strTableName);

		// Get Total number of records in Table
		int tableSize = 0;
		if (table.getPageCount() > 0) {
			tableSize = (table.getPageCount() - 1) * dbHelper.getMaximumRowsCountInPage();
			tableSize += table.readPage(dbHelper.getPagePath(strTableName, table.getPageCount() - 1)).getSize();
		}

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
		System.out.println("yalla Mada fa");
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

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
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
		// ------------------------------------------------------valid data
		int start_read = 0;
		int end_read = table.getPageCount();
		int counter = 0;
		boolean keychanged = false;
		Hashtable<String, Object> tmp = new Hashtable<String, Object>();
		while (start_read < end_read) {
			Page curPage = table
					.readPage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_" + start_read++);
			Vector<Record> v = curPage.getPage();
			for (int i = 0; i < curPage.getSize(); i++) {
				if ((v.get(i).getPrimaryKey() + "").equals(strKey)) {
					counter++;
					Hashtable<String, Object> old = v.get(i).getRecord();
					for (Entry<String, Object> e : htblColNameValue.entrySet()) {
						String colName = e.getKey();
						Object value = e.getValue();
						if (colName.equals(v.get(i).getPrimaryKeyCN())) {
							if (!strKey.equals(value)) {
								keychanged = true;
								tmp.put(v.get(i).getPrimaryKeyCN(), old.get(v.get(i).getPrimaryKeyCN()));
							}
						}
						if (old.containsKey(colName)) {
							old.put(colName, htblColNameValue.get(colName));
						}

						old.put("TouchDate", dbHelper.currentDate());
					}
					if (keychanged) {
						keychanged = false;
						old.remove("TouchDate");
						deleteFromTable(strTableName, tmp);
						insertIntoTable(strTableName, old);
					} else {
						table.writePage(dbHelper.getDBPath() + "/data/" + strTableName + "/" + strTableName + "_"
								+ (start_read - 1), curPage);
					}

				}
			}

		}
		if (counter == 0) {
			throw new DBAppException("There is no entry with this key");
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
		boolean added = false;
		Page curPage = null;
		// trying to add the record sequentially
		while (start < end && !added) {
			curPage = table.readPage(dbHelper.getPagePath(strTableName, start));
			added |= curPage.addRecord(record, dbHelper.getMaximumRowsCountInPage());
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
			Page curPage = table.readPage(dbHelper.getPagePath(strTableName, start_read++));

			Vector<Record> v = curPage.getPage();
			for (int i = 0; i < curPage.getSize(); i++) {
				if (!dbHelper.matchRecord(v.get(i).getRecord(), htblColNameValue)) {
					writePage.addRecord(v.get(i), dbHelper.getMaximumRowsCountInPage());
					if (writePage.getSize() == dbHelper.getMaximumRowsCountInPage()) {
						table.writePage(dbHelper.getPagePath(strTableName, start_write++), writePage);
						writePage = new Page();
					}
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

	public DBHelper getDbHelper() {
		return dbHelper;
	}

	public Hashtable<String, Table> getTables() {
		return tables;
	}
}
