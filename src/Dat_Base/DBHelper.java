package Dat_Base;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;

public class DBHelper {
	
	private String DBPath = "/Users/abdulrahmanibrahim/eclipse-workspace/DBMS/";
	private int MaximumRowsCountInPage;
	
	public DBHelper() {
		int MaximumRowsCountInPage = 200;
		try{
			BufferedReader buffer = new BufferedReader(new FileReader(DBPath + "config/DBApp.config"));
			while(buffer.ready()) {
				String[] prop = buffer.readLine().split(" : ");
				String propName = prop[0];
				
				if(prop.length < 2) continue;
				
				int propValue = Integer.parseInt(prop[1]);
				if(propName.equals("MaximumRowsCountInPage")) {
					MaximumRowsCountInPage = propValue;
				}
			}
			buffer.close();
		} catch(IOException e) {
			System.out.println("Error Loading DB Config");
			e.printStackTrace(System.err);
		} finally {
			this.MaximumRowsCountInPage = MaximumRowsCountInPage;
			
		}
	}
	
	public Object reflect(String type) throws Exception {
		switch(type) {
		case "java.lang.Integer": return 0;
		case "java.lang.String": return "";
		case "java.lang.Double": return 0.0;
		case "java.lang.Boolean": return false;
		case "java.util.Date": return new Date();
		}
		System.err.printf("Error: Problem reflecting %s, undefined Type\n", type);
		return null;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes"})
	public Object reflect(String type, String value) throws Exception {
		try {
			Class _class = Class.forName(type);
			Constructor constructor = _class.getConstructor(type.getClass());
			return constructor.newInstance(value);
		} catch(Exception e) {
			System.out.printf("Error: Problem reflecting %s with value %s\n", type, value);
			e.printStackTrace(System.err);
			return null;
		}
	}
	
	private boolean validDate(int year, int month, int day) {
		return year > 0 && month >= 0 && month < 12 && day > 0 && day <= 31;
	}
	
	private boolean validTime(int hour, int min, int sec) {
		return hour > 0 && hour <= 24 && min >= 0 && min <= 60 && sec > 0 && sec <= 60;
	}
	
	@SuppressWarnings("deprecation")
	public Date createDate(int year, int month, int day) {
		if(validDate(year, month, day))
			return new Date(year - 1900, month - 1, day);
		System.err.println("Invalid Date Fields");
		return null;
	}
	
	@SuppressWarnings("deprecation")
	public Date createDateTime(int year, int month, int day, int hour, int min, int sec) {
		if(validDate(year, month, day) && validTime(hour, min, sec))
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
