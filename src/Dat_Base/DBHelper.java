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

public class DBHelper {

	private String DBPath = "/Users/abdulrahmanibrahim/eclipse-workspace/DBMS/";
	private int MaximumRowsCountInPage;

	public DBHelper() {
		int MaximumRowsCountInPage = 200;
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
				}
			}
			buffer.close();

			// Clearing Metadata file
			PrintWriter metadataWriter = new PrintWriter(DBPath + "data/metadata.csv");
			metadataWriter.close();

		} catch (IOException e) {
			System.err.println("Error Loading DB Config and Metadata");
			e.printStackTrace(System.err);
		} finally {
			this.MaximumRowsCountInPage = MaximumRowsCountInPage;
		}
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

	public Hashtable<String, Object> getTableColNameType(String tableName) throws IOException {
		BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "data/metadata.csv"));
		String line, tokens[];
		Hashtable<String, Object> htbColNameType = new Hashtable<>();
		while (buffer.ready()) {
			line = buffer.readLine();
			tokens = line.split(",");
			if (tokens[0].equals(tableName))
				htbColNameType.put(tokens[1], reflect(tokens[3]));
		}
		buffer.close();
		return htbColNameType;
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

	public int getMaximumRowsCountInPage() {
		return MaximumRowsCountInPage;
	}

}
