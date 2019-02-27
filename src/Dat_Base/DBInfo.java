package Dat_Base;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DBInfo {
	
	private String DBPath = "/Users/abdulrahmanibrahim/eclipse-workspace/DBMS/";
	private int MaximumRowsCountInPage;
	
	public DBInfo() {
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
			e.printStackTrace();
		} finally {
			this.MaximumRowsCountInPage = MaximumRowsCountInPage;
			
		}
	}

	public String getDBPath() {
		return DBPath;
	}

	public int getMaximumRowsCountInPage() {
		return MaximumRowsCountInPage;
	}
	
}
