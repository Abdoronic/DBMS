package Dat_Base;

import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {

	private static final long serialVersionUID = 1L;
	private Vector<Record> page;

	public Page() {
		this.page = new Vector<>();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public int addRecord(Record record, int maximumRowsCountInPage) throws DBAppException {
		Comparable insertedKey = (Comparable) record.getPrimaryKey();
		if (page.size() == 0) {
			page.add(0, record);
			return 0;
		}
		for (int i = 0; i < page.size(); i++) {
			Record currRecord = page.get(i);
			Comparable currKey = (Comparable) currRecord.getPrimaryKey();
			if (insertedKey.compareTo(currKey) < 0) {
				page.add(i, record);
				return i;
			} else if (insertedKey.compareTo(currKey) == 0) {
				throw new DBAppException("The PrimaryKey ( " + currKey + " ) already exists");
			}
		}
		if (page.size() < maximumRowsCountInPage) {
			page.add(page.size(), record);
			return page.size() - 1;
		}
		return -1;
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
	
	public Record getRecord(int index) {
		return page.get(index);
	}

	public int getSize() {
		return page.size();
	}

	@Override
	public String toString() {
		String s = "start \n";
		for (Record r : page)
			s += r.toString() + "\n";
		return s + "end";
	}
}
