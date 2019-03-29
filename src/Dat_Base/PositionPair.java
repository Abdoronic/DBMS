package Dat_Base;

public class PositionPair {
	private int pageNumber, recordIndex;
	
	public PositionPair(int pageNumber, int recordIndex) {
		this.pageNumber = pageNumber;
		this.recordIndex = recordIndex;
	}
	
	public int getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	public int getRecordIndex() {
		return recordIndex;
	}

	public void setRecordIndex(int recordIndex) {
		this.recordIndex = recordIndex;
	}

	@Override
	public String toString() {
		return "PositionPair [pageNumber=" + pageNumber + ", recordIndex=" + recordIndex + "]";
	}
	
}
