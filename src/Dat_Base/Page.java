package Dat_Base;

import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {

	private static final long serialVersionUID = 1L;
	private Vector<Record> page;

	public Page() {
		this.page = new Vector<>();
	}
//<<<<<<< HEAD
//	// Page page2=ObjectReader(path);
//	// page2.addRecord(htblColNameValue);
//=======
//
//>>>>>>> 086a7631be2aac84f306923a58b3b4599d7141cd
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean addRecord(Record record) {
		Comparable insertedKey = (Comparable) record.getPrimaryKey();
		for (int i = 0; i < page.size(); i++) {
			Record currRecord = page.get(i);
			Comparable currKey = (Comparable) currRecord.getPrimaryKey();
			if (insertedKey.compareTo(currKey) < 0) {
				page.add(i, record);
				return true;
			}
		}
		return false;
	}

	public Vector<Record> getRecords(String colName, Object value) {
		Vector<Record> res = new Vector<>();
		for (int i = 0; i < page.size(); i++) {
			Record currRecord = page.get(i);
			Object currValue = currRecord.getCell(colName);
			if (currValue.equals(value)) {
				res.add(currRecord);
			}
		}
		return res;
	}

	public Vector<Record> getPage() {
		return page;
	}

	public int getSize() {
		return page.size();
	}
}
