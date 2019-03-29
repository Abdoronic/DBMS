package Dat_Base;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Stack;
import java.util.Vector;

public class BitMap {

	private String tableName;
	private String colName;
	private int indexPageCount;
	private DBHelper dbHelper;
	private QueryManager queryManager;
	
	/**
	 * 
	 * @param tableName The name of the table that has the index
	 * @param colName The name of the column that the index is built on
	 * @param dbHelper The dbHelper Class
	 */

	public BitMap(String tableName, String colName, DBHelper dbHelper) {
		this.tableName = tableName;
		this.colName = colName;
		this.indexPageCount = createFolderAndCountPages();
		this.dbHelper = dbHelper;
		this.queryManager = null;
	}
	
	public BitMap(String tableName, String colName, DBHelper dbHelper, QueryManager queryManager) {
		this.tableName = tableName;
		this.colName = colName;
		this.indexPageCount = createFolderAndCountPages();
		this.dbHelper = dbHelper;
		this.queryManager = queryManager;
	}
	
	/**
	 * Creates a folder for the index if it didn't exist
	 * @return returns the number of pages in case the Index was created previously
	 */

	public int createFolderAndCountPages() {
		File theDir = new File("./data/" + tableName + "_" + colName + "_Index" + "/");
		// if the directory does not exist, create it
		if (!theDir.exists()) {
			System.out.println("creating directory: " + theDir.getName());
			try {
				theDir.mkdir();
			} catch (SecurityException e) {
				e.printStackTrace(System.err);
			}
			System.out.printf("DIR %s created\n", tableName);
		}
		return theDir.listFiles().length;
	}
	
	/**
	 * Reads a page of a Bitmap index
	 * @param path The path of the Index page it needs to read
	 * @return The page as an IndexPage object
	 */

	public IndexPage readPage(String path) {
		try {
			FileInputStream fstream = new FileInputStream(path);
			ObjectInputStream ois = new ObjectInputStream(fstream);
			IndexPage p = (IndexPage) ois.readObject();
			ois.close();
			p.decode();
			return p;
		} catch (Exception e) {
			System.err.println("Error Reading from Page");
			e.printStackTrace(System.err);
		}
		return null;
	}
	
	/**
	 * 
	 * @param path The path of the Index page it needs to read
	 * @param page IndexPage object to be written
	 */

	public void writePage(String path, IndexPage page) {
		try {
			FileOutputStream fstream = new FileOutputStream(new File(path));
			ObjectOutputStream oos = new ObjectOutputStream(fstream);
			page.encode();
			oos.writeObject(page);
			oos.close();
		} catch (IOException e) {
			System.err.println("Error Writing to file");
			e.printStackTrace(System.err);
		}
	}
	
	public void pushUp(int start, int maxRowsPerPage) {
		int end = start + 1;
		IndexPage startPage = readPage(dbHelper.getIndexPagePath(tableName, colName, 0));
		while(start < indexPageCount && end < indexPageCount) {
			if(start == end) {
				end++;
				continue;
			}
			startPage = readPage(dbHelper.getIndexPagePath(tableName, colName, start));
			IndexPage endPage = readPage(dbHelper.getIndexPagePath(tableName, colName, end));
			int startPageSize = startPage.getSize();
			int endPageSize = endPage.getSize();
			int needed = maxRowsPerPage - startPageSize;
			if(startPageSize < maxRowsPerPage) {
				if(endPageSize > 0) {
					int take = Math.min(endPageSize, needed);
					Stack<IndexPair> stack = new Stack<>();
					while(!endPage.getIndexPage().isEmpty())
						stack.push(endPage.getIndexPage().remove(endPage.getSize() - 1));
					while(take-- > 0) {
						startPage.getIndexPage().add(stack.pop());
						needed--;
					}
					if(needed == 0) {
						writePage(dbHelper.getIndexPagePath(tableName, colName, start), startPage);
						start++;
					}
					while(!stack.isEmpty())
						endPage.getIndexPage().add(stack.pop());
				} else {
					end++;
				}
			} else {
				writePage(dbHelper.getIndexPagePath(tableName, colName, start), startPage);
				start++;
			}
		}
		if(start < indexPageCount && startPage.getSize() > 0)
			writePage(dbHelper.getIndexPagePath(tableName, colName, start++), startPage);
		
		while (start < indexPageCount) { // delete extra pages
			File file = new File(dbHelper.getIndexPagePath(tableName, colName, start++));
			file.delete();
		}
	}
	
	public void pushDown(int start, int maxRowsPerPage) {
		for(; start < indexPageCount; start++) {
			IndexPage startPage = readPage(dbHelper.getIndexPagePath(tableName, colName, start));
			int startPageSize = startPage.getSize();
			if(startPageSize > maxRowsPerPage) {
				if(start < indexPageCount - 1) {
					IndexPage nextPage = readPage(dbHelper.getIndexPagePath(tableName, colName, start + 1));
					nextPage.getIndexPage().add(0, startPage.getIndexPage().remove(startPage.getSize() - 1));
					writePage(dbHelper.getIndexPagePath(tableName, colName, start + 1), nextPage);
				} else {
					IndexPage lastPage = new IndexPage();
					lastPage.getIndexPage().add(startPage.getIndexPage().remove(startPage.getSize() - 1));
					writePage(dbHelper.getIndexPagePath(tableName, colName, start + 1), lastPage);
				}
			}
			writePage(dbHelper.getIndexPagePath(tableName, colName, start), startPage);
		}
	}
	
	/**
	 * 
	 * @param tableName The name of the table which the Bitmap Index is created on
	 * @param colName The name of the column which the Bitmap index is created on
	 * @param insertedValue the key value to be inserted in the Bitmap 
	 * @param The index in which the record was inserted at                               
	 * @return returns true if the record got inserted in the Bitmap successfully
	 */
	
	public boolean insertIntoBitMap(Comparable<Object> insertedValue, int insertedIndex, boolean newRecordAdded) {
		int tableSize = dbHelper.getTableSize(tableName);
		int MaxIndexPairsPerPage = dbHelper.getBitmapSize();
		int start = 0;
		boolean added = false;
		IndexPage currPage, nextPage;
		while(start < indexPageCount && !added) {
			currPage = readPage(dbHelper.getIndexPagePath(tableName, colName, start));
			added |= currPage.addValueToIndex(insertedValue, insertedIndex, tableSize, MaxIndexPairsPerPage, newRecordAdded);
			writePage(dbHelper.getIndexPagePath(tableName, colName, start), currPage);
			++start;
		}
		for(int i = start; i < indexPageCount && added; i++) {
			currPage = readPage(dbHelper.getIndexPagePath(tableName, colName, i));
			currPage.paddBits(insertedIndex, insertedValue, newRecordAdded);
		}
		nextPage = null;
		if(start < indexPageCount)
			nextPage = readPage(dbHelper.getIndexPagePath(tableName, colName, start));
		while(start < indexPageCount - 1 && added) {
			currPage = nextPage;
			if(currPage.getSize() > MaxIndexPairsPerPage) {
				IndexPair lastPair = currPage.getIndexPage().remove(currPage.getSize() - 1);
				writePage(dbHelper.getIndexPagePath(tableName, colName, start), currPage);
				nextPage = readPage(dbHelper.getIndexPagePath(tableName, colName, start + 1));
				nextPage.getIndexPage().add(0, lastPair);
				++start;
			} else {
				break;
			}
		}
		if(nextPage != null && nextPage.getSize() > MaxIndexPairsPerPage) {
			Vector<IndexPair> newLastPageData = new Vector<>();
			newLastPageData.add(nextPage.getIndexPage().remove(nextPage.getSize() - 1));
			IndexPage newPage = new IndexPage(newLastPageData);
			writePage(dbHelper.getIndexPagePath(tableName, colName, start), nextPage);
			writePage(dbHelper.getIndexPagePath(tableName, colName, start + 1), newPage);
		}
		if(!added) {
			Vector<IndexPair> newLastPageData = new Vector<>();
			IndexPair newIndexPair = new IndexPair(insertedValue, tableSize);
			newIndexPair.set(insertedIndex);
			newLastPageData.add(newIndexPair);
			IndexPage newPage = new IndexPage(newLastPageData);
			writePage(dbHelper.getIndexPagePath(tableName, colName, Math.min(start + 1, indexPageCount)), newPage);
		}
		return true;
	}
	
	public boolean deleteFromBitMap(int insertedIndex) {
		for(int i = 0; i < indexPageCount; i++) {
			IndexPage currPage = readPage(dbHelper.getIndexPagePath(tableName, colName, i));
			currPage.deleteBits(insertedIndex);
			writePage(dbHelper.getIndexPagePath(tableName, colName, i), currPage);
		}
		return true;
	}
	
	public void updateBitMap(Comparable<Object> oldValue, Comparable<Object> insertedValue, int insertedIndex) {
		boolean insertedValueExist = false;
		for(int i = 0; i < indexPageCount; i++) {
			IndexPage currPage = readPage(dbHelper.getIndexPagePath(tableName, colName, i));
			for(int j = 0; j < currPage.getSize(); j++) {
				if(currPage.getIndexPair(j).getValue().compareTo(oldValue) == 0)
					currPage.getIndexPair(j).reset(insertedIndex);
				if(currPage.getIndexPair(j).getValue().compareTo(insertedValue) == 0) {
					currPage.getIndexPair(j).set(insertedIndex);
					insertedValueExist = true;
				}
			}
			writePage(dbHelper.getIndexPagePath(tableName, colName, i), currPage);
		}
		if(!insertedValueExist)
			insertIntoBitMap(insertedValue, insertedIndex, false);
	}

	public String getTableName() {
		return tableName;
	}

	public String getColName() {
		return colName;
	}

	public int getPageCount() {
		return indexPageCount;
	}

	public void setPageCount(int pageCount) {
		this.indexPageCount = pageCount;
	}

	public void incPageCount() {
		this.indexPageCount++;
	}

}
