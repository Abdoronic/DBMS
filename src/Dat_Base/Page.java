package Dat_Base;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;


public class Page implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Vector<Hashtable<String, Object>> page;
	private String primaryKey;
	
	public Page(String primaryKey) {
		this.page = new Vector<>();
		this.primaryKey = primaryKey;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void addRecord(Hashtable<String, Object> record) {
		Comparable insertedKey = (Comparable)record.get(primaryKey);
		for(int i = 0; i < page.size(); i++) {
			Hashtable<String, Object> currRecord = page.get(i);
			Comparable currKey = (Comparable)currRecord.get(primaryKey);
			if(insertedKey.compareTo(currKey) < 0) {
				page.add(i, record);
				return;
			}
		}
		page.add(record);
	}
	
	public Vector<Hashtable<String, Object>> getRecords(String key, Object value) {
		Vector<Hashtable<String, Object>> res = new Vector<>();
		for(int i = 0; i < page.size(); i++) {
			Hashtable<String, Object> currRecord = page.get(i);
			Object currValue = currRecord.get(key);
			if(currValue.equals(value)) {
				res.add(currRecord);
			}
		}
		return res;
	}
	
	public int getSize() {
		return page.size();
	}
}
