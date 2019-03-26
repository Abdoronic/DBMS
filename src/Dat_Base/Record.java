package Dat_Base;

import java.io.Serializable;
import java.util.Hashtable;

public class Record implements Serializable, Comparable<Record> {

	private static final long serialVersionUID = 1L;
	private String primaryKey;
	private Hashtable<String, Object> record;

	public Record(String primarKey, Hashtable<String, Object> record) {
		this.primaryKey = primarKey;
		this.record = record;
	}

	public Object getPrimaryKey() {
		return record.get(primaryKey);
	}

	public String getPrimaryKeyCN() {
		return this.primaryKey;
	}

	public Object getCell(String colName) {
		return record.get(colName);
	}

	public Hashtable<String, Object> getRecord() {
		return record;
	}

	@Override
	public String toString() {
		return record.toString() + " PK= " + primaryKey;
	}

	@Override
	@SuppressWarnings("unchecked")
	public int compareTo(Record o) {
		return ((Comparable<Object>)getPrimaryKey()).compareTo((Comparable<Object>)o.getPrimaryKey());
	}
}
