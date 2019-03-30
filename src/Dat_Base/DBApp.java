package Dat_Base;

import java.io.IOException;
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
		BitMap bitMap = new BitMap(strTableName, strColName, dbHelper);
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
								BitMap colBitMap = new BitMap(strTableName, colName, dbHelper);
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

	public boolean insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		Table table = dbHelper.getTable(strTableName);
		if (table == null)
			throw new DBAppException("Table is not found!");

		String primaryKeyColName = "";

		try {
			primaryKeyColName = dbHelper.getTableKey(strTableName);
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (!htblColNameValue.containsKey(primaryKeyColName))
			throw new DBAppException("Primary Key <" + primaryKeyColName + "> is not provided");

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

		// Valid Data to be inserted

		htblColNameValue.put("TouchDate", dbHelper.currentDate());
		Record record = new Record(primaryKeyColName, htblColNameValue);

		int insertedIndex = queryManager.insertIntoTable(strTableName, record);

		if (insertedIndex == -1)
			throw new DBAppException("Duplicate Key in Record " + record.toString());

		for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
			if (dbHelper.isIndexed(strTableName, e.getKey()))
				queryManager.insertIntoBitMap(strTableName, e.getKey(), e.getValue(), insertedIndex);
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
		QueryManager queryManager = new QueryManager(dbHelper);
		queryManager.deleteFromTable(strTableName, htblColNameValue);
	}

	public Iterator<Record> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		if (arrSQLTerms.length == 0)
			throw new DBAppException("Invalid Where Clause");
		String tableName = arrSQLTerms[0].getTableName();
		if (!tables.containsKey(tableName))
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

		if (table.getPageCount() == 0)
			return result.iterator();

		QueryManager queryManager = new QueryManager(dbHelper);

		String bits = queryManager.getSearchSpace(arrSQLTerms, strarrOperators);

		int maximumRowsCountInPage = dbHelper.getMaximumRowsCountInPage();

		int pc = 0;
		Page currPage = table.readPage(dbHelper.getPagePath(tableName, pc));
		for (int i = 0; i < bits.length(); i++) {
			if (bits.charAt(i) == '0')
				continue;
			int recordPage = i / maximumRowsCountInPage;
			int recordIndex = i % maximumRowsCountInPage;
			if (recordPage > pc)
				currPage = table.readPage(dbHelper.getPagePath(tableName, ++pc));
			if (queryManager.verfiyWhereClause(arrSQLTerms, strarrOperators, currPage.getRecord(recordIndex)))
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
