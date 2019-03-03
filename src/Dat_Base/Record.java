package Dat_Base;

import java.util.Hashtable;

public class Record {
	private String primaryKey;
	private Hashtable<String, Object> record;
	
	public Record(String primarKey, Hashtable<String, Object> record) {
		this.primaryKey = primarKey;
		this.record = record;
	}
	
	public Object getPrimaryKey() {
		return record.get(primaryKey);
	}
	
	public Object getCell(String colName) {
		return record.get(colName);
	}
	
	public Hashtable<String, Object> getRecord() {
		return record;
	}
	
}
