package Dat_Base;

import java.lang.reflect.Constructor;

public class DBApp {
	
	private DBInfo dbInfo;
	
	public DBApp() {
		init();
	}
	
	public void init() {
		dbInfo = new DBInfo();
		
	}
	
	public static void main(String[] args) throws Exception{
//		DBApp db = new DBApp();
		
		String strColType = "java.lang.Integer";
		String strColValue = "100";
		
		@SuppressWarnings("rawtypes")
		Class cls = Class.forName(strColType);
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Constructor constructor = cls.getConstructor(strColType.getClass());
		
		Object x = constructor.newInstance(strColValue); 
		System.out.println(x);
		System.out.println(x instanceof Integer);
	}
	
}
