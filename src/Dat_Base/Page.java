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
	public boolean addRecord(Record record , int MAX) throws DBAppException{
		Comparable insertedKey = (Comparable) record.getPrimaryKey();
		if(page.size() == 0)
		{
			page.add(0, record);
			return true;
		}
		for (int i = 0; i < page.size(); i++) {
			Record currRecord = page.get(i);
			Comparable currKey = (Comparable) currRecord.getPrimaryKey();
			if (insertedKey.compareTo(currKey) < 0) {
				page.add(i, record);
				return true;
			}
			else if(insertedKey.compareTo(currKey) == 0)
			{
				throw new DBAppException("The PrimaryKey ( "+currKey+" ) already exists");
			}
		}
		if(page.size() < MAX )
		{
			page.add(page.size(), record);
			return true;			
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
	
	@Override
	public String toString() {
		String s = "start \n";
		for(Record r:page)
			s += r.toString()+"\n";
		return s+"end";
	}
}
