package Dat_Base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;

public class DBHelper {

	private String DBPath = new File("").getAbsolutePath() + "/";
	private int MaximumRowsCountInPage;
	private int BitmapSize;

	public DBHelper() {
		DBPath = new File("").getAbsolutePath() + "/";
		MaximumRowsCountInPage = 200;
		BitmapSize = 150;
		loadConfigurations();
	}

	public void loadConfigurations() {
		try {
			// Loading DB Configuration file
			BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "config/DBApp.config"));
			while (buffer.ready()) {
				String[] prop = buffer.readLine().split(" : ");
				String propName = prop[0];

				if (prop.length < 2)
					continue;

				int propValue = Integer.parseInt(prop[1]);
				if (propName.equals("MaximumRowsCountInPage")) {
					MaximumRowsCountInPage = propValue;
				} else if (propName.equals("BitmapSize")) {
					BitmapSize = propValue;
				}
			}
			buffer.close();

		} catch (IOException e) {
			System.err.println("Error Loading DB Config");
			e.printStackTrace(System.err);
		}
	}

	public Hashtable<String, Table> getTables() {
		Hashtable<String, Table> tables = new Hashtable<>();
		try {
			BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "data/metadata.csv"));
			String[] tokens;
			while (buffer.ready()) {
				tokens = buffer.readLine().split(",");
				tables.put(tokens[0], new Table(tokens[0], this));
			}
			buffer.close();
		} catch (IOException e) {
			System.err.println("Cannot Load tables from Metadata");
			e.printStackTrace();
		}
		return tables;
	}
	
	public Table getTable(String tableName) {
		Table table = null;
		try {
			BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "data/metadata.csv"));
			String[] tokens;
			while (buffer.ready()) {
				tokens = buffer.readLine().split(",");
				if(tokens[0].equals(tableName)) {
					table = new Table(tableName, this);
					break;
				} 
			}
			buffer.close();
		} catch (IOException e) {
			System.err.println("Cannot Load tables from Metadata");
			e.printStackTrace();
		}
		return table;
	}
	
	public int getTableSize(String tableName) {
		Table table = getTable(tableName);
		if(table != null)
			return calcTableSize(table);
		return -1;
	}
	
	public int calcTableSize(Table table) {
		if(table.getPageCount() > 0) {
			int tableSize = (table.getPageCount() - 1) * getMaximumRowsCountInPage();
			tableSize += table.readPage(getPagePath(table.getTableName(), table.getPageCount() - 1)).getSize();
			return tableSize;
		}
		return 0;
	}
	
	public int getIndexSize(String tableName, String colName) {
		BitMap bitMap = new BitMap(tableName, colName, this);
		return calcIndexSizeSize(bitMap);
	}
	
	public int calcIndexSizeSize(BitMap bitMap) {
		if(bitMap.getPageCount() > 0) {
			int indexSize = (bitMap.getPageCount() - 1) * getBitmapSize();
			indexSize += bitMap.readPage(getIndexPagePath(bitMap.getTableName(), bitMap.getColName(), bitMap.getPageCount() - 1)).getSize();
			return indexSize;
		}
		return 0;
	}

	public void addToMetaData(String tableName, String primaryKey, Hashtable<String, String> colNameType)
			throws IOException {
		File metadata = new File(DBPath + "data/metadata.csv");
		FileWriter fileWriter = new FileWriter(metadata.getAbsolutePath(), true);
		PrintWriter metadataWriter = new PrintWriter(fileWriter);
		for (Map.Entry<String, String> e : colNameType.entrySet()) {
			String isKey = e.getKey().equals(primaryKey) ? "True" : "False";
			metadataWriter.printf("%s,%s,%s,%s,False\n", tableName, e.getKey(), e.getValue(), isKey);
		}
		metadataWriter.close();
	}

	public Object reflect(String type) {
		switch (type) {
		case "java.lang.Integer":
			return 0;
		case "java.lang.String":
			return "";
		case "java.lang.Double":
			return 0.0;
		case "java.lang.Boolean":
			return false;
		case "java.util.Date":
			return new Date();
		}
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object reflect(String type, String value) throws Exception {
		try {
			Class _class = Class.forName(type);
			Constructor constructor = _class.getConstructor(type.getClass());
			return constructor.newInstance(value);
		} catch (Exception e) {
			System.out.printf("Error: Problem reflecting %s with value %s\n", type, value);
			e.printStackTrace(System.err);
			return null;
		}
	}

	public boolean isTypeSupported(String type) {
		return reflect(type) != null;
	}

	public String getTableKey(String tableName) throws IOException {
		BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "data/metadata.csv"));
		String key = null, line, tokens[];
		while (buffer.ready()) {
			line = buffer.readLine();
			tokens = line.split(",");
			if (tokens[0].equals(tableName) && tokens[3].equals("True")) {
				key = tokens[1];
				break;
			}
		}
		buffer.close();
		return key;
	}

	public Boolean isIndexed(String tableName, String colName) {
		try {
			BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "data/metadata.csv"));
			String line, tokens[];
			boolean isIndexed = false;
			while (buffer.ready()) {
				line = buffer.readLine();
				tokens = line.split(",");
				if (tokens[0].equals(tableName) && tokens[1].equals(colName)) {
					isIndexed = tokens[4].equals("True");
					break;
				}
			}
			buffer.close();
			return isIndexed;
		} catch (IOException e) {
			System.err.println("Error Checking index from metadata");
			e.printStackTrace(System.err);
			return false;
		}
	}

	public void setIndexed(String tableName, String colName) {
		try {
			StringBuilder newMetaData = new StringBuilder();
			BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "data/metadata.csv"));
			String line, tokens[];
			while (buffer.ready()) {
				line = buffer.readLine();
				tokens = line.split(",");
				if (tokens[0].equals(tableName) && tokens[1].equals(colName)) {
					line = "";
					for (int j = 0; j < tokens.length - 1; j++)
						line += tokens[j] + ',';
					line += "True";
				}
				newMetaData.append(line + '\n');
			}
			buffer.close();
			File metadata = new File(DBPath + "data/metadata.csv");
			FileWriter fileWriter = new FileWriter(metadata.getAbsolutePath());
			PrintWriter metadataWriter = new PrintWriter(fileWriter);
			metadataWriter.print(newMetaData.toString());
			metadataWriter.close();
		} catch (IOException e) {
			System.err.println("Error Changing index from metadata");
			e.printStackTrace(System.err);
		}
	}

	public Hashtable<String, Object> getTableColNameType(String tableName) throws IOException {
		BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "data/metadata.csv"));
		String line, tokens[];
		Hashtable<String, Object> htbColNameType = new Hashtable<>();
		while (buffer.ready()) {
			line = buffer.readLine();
			tokens = line.split(",");
			if (tokens[0].equals(tableName))
				htbColNameType.put(tokens[1], reflect(tokens[2]));
		}
		buffer.close();
		return htbColNameType;
	}

	public boolean matchRecord(Hashtable<String, Object> fromTable, Hashtable<String, Object> attributSet) {
		for (Entry<String, Object> e : attributSet.entrySet()) {
			String colName = e.getKey();
			Object value = e.getValue();
			if (!fromTable.get(colName).equals(value))
				return false;
		}
		return true;
	}

	private boolean validDate(int year, int month, int day) {
		return year > 0 && month >= 0 && month < 12 && day > 0 && day <= 31;
	}

	private boolean validTime(int hour, int min, int sec) {
		return hour > 0 && hour <= 24 && min >= 0 && min <= 60 && sec > 0 && sec <= 60;
	}

	@SuppressWarnings("deprecation")
	public Date createDate(int year, int month, int day) {
		if (validDate(year, month, day))
			return new Date(year - 1900, month - 1, day);
		System.err.println("Invalid Date Fields");
		return null;
	}

	@SuppressWarnings("deprecation")
	public Date createDateTime(int year, int month, int day, int hour, int min, int sec) {
		if (validDate(year, month, day) && validTime(hour, min, sec))
			return new Date(year - 1900, month - 1, day, hour, min, sec);
		System.err.println("Invalid DateTime Fields");
		return null;
	}

	public Date currentDate() {
		return new Date();
	}

	public String getDBPath() {
		return DBPath;
	}

	/**
	 * 
	 * @param tableName
	 * @param pageNumber
	 * @return The path the page is at or should be at
	 */
	public String getPagePath(String tableName, int pageNumber) {
		return DBPath + "/data/" + tableName + "/" + tableName + "_" + pageNumber;
	}

	public String getIndexPagePath(String tableName, String colName, int pageNumber) {
		return DBPath + "/data/" + tableName + "_" + colName + "_Index" + "/" + tableName + "_" + colName + "_"
				+ pageNumber;
	}

	public int getMaximumRowsCountInPage() {
		return MaximumRowsCountInPage;
	}

	public int getBitmapSize() {
		return BitmapSize;
	}

}
