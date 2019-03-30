package Dat_Base;

import java.io.Serializable;
import java.util.Vector;

public class IndexPage implements Serializable {
	private static final long serialVersionUID = 1L;

	private Vector<IndexPair> indexPage;

	public IndexPage() {
		indexPage = new Vector<>();
	}

	public IndexPage(Vector<IndexPair> indexPage) {
		this.indexPage = indexPage;
	}

	public boolean paddBits(int index, Comparable<Object> insertedValue, boolean newRecordAdded) {
		boolean exist = false;
		for (int i = 0; i < indexPage.size(); i++) {
			if (indexPage.get(i).getValue().compareTo(insertedValue) == 0) {
				if (newRecordAdded)
					indexPage.get(i).insert(index, "1");
				else
					indexPage.get(i).set(index);
				exist = true;
			} else {
				if (newRecordAdded)
					indexPage.get(i).insert(index, "0");
			}
		}
		return exist;
	}

	public boolean deleteBits(int index) {
		boolean removed = false;
		for (int i = 0; i < indexPage.size(); i++) {
			indexPage.get(i).delete(index);
			boolean hasOne = false;
			for (char c : indexPage.get(i).getBits().toCharArray()) {
				if (c == '1') {
					hasOne = true;
					break;
				}
			}
			if (!hasOne) {
				indexPage.remove(i--);
				removed = true;
			}
		}
		return removed;
	}

	public void encode() {
		for (int i = 0; i < indexPage.size(); i++)
			indexPage.get(i).encode();
	}

	public void decode() {
		for (int i = 0; i < indexPage.size(); i++)
			indexPage.get(i).decode();
	}

	public Vector<IndexPair> getIndexPage() {
		return indexPage;
	}

	public IndexPair getIndexPair(int index) {
		return indexPage.get(index);
	}

	public int getSize() {
		return indexPage.size();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (IndexPair ip : indexPage)
			sb.append(ip.toString() + "\n");
		return sb.toString();
	}

}
