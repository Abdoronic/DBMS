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

	public boolean addValueToIndex(Comparable<Object> insertedValue, int insertedIndex, int tableSize,
			int maxPairsPerPage, boolean newRecordAdded) {
		if (paddBits(insertedIndex, insertedValue, newRecordAdded))
			return true; // will need to continue padding zeroes to the rest of pages
		IndexPair newIndexPair = new IndexPair(insertedValue, tableSize);
		newIndexPair.set(insertedIndex);
		if (indexPage.isEmpty())
			return indexPage.add(newIndexPair);
		for (int i = 0; i < indexPage.size(); i++) {
			if (newIndexPair.compareTo(indexPage.get(i)) < 0) {
				indexPage.add(i, newIndexPair);
				return true;
			}
		}
		if (indexPage.size() < maxPairsPerPage)
			return indexPage.add(newIndexPair);
		return false;
	}

	public boolean paddBits(int index, Comparable<Object> insertedValue, boolean newRecordAdded) {
		boolean exist = false;
		for (int i = 0; i < indexPage.size(); i++) {
			if (indexPage.get(i).getValue().equals(insertedValue)) {
				if(newRecordAdded)
					indexPage.get(i).insert(index, "1");
				else
					indexPage.get(i).set(index);
				exist = true;
			} else {
				if(newRecordAdded)
					indexPage.get(i).insert(index, "0");
			}
		}
		return exist;
	}

	public void deleteBits(int index) {
		for (int i = 0; i < indexPage.size(); i++)
			indexPage.get(i).delete(index);
	}
	
	public void encode() {
		for(int i = 0; i < indexPage.size(); i++)
			indexPage.get(i).encode();
	}
	
	public void decode() {
		for(int i = 0; i < indexPage.size(); i++)
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
