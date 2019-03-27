package Dat_Base;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Stack;

public class Table {

	private String tableName;
	private int pageCount;
	private DBHelper dbHelper;

	public Table(String tableName, DBHelper dbHelper) {
		this.tableName = tableName;
		this.pageCount = createFolderAndCountPages();
		this.dbHelper = dbHelper;
	}

	public int createFolderAndCountPages() {
		File theDir = new File("./data/" + tableName);
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

	public Page readPage(String path) {
		try {
			FileInputStream fstream = new FileInputStream(path);
			ObjectInputStream ois = new ObjectInputStream(fstream);
			Page p = (Page) ois.readObject();
			ois.close();
			return p;
		} catch (Exception e) {
			System.err.println("Error Reading from Page");
			e.printStackTrace(System.err);
		}
		return null;
	}

	public void writePage(String path, Page page) {
		try {
			FileOutputStream fstream = new FileOutputStream(new File(path));
			ObjectOutputStream oos = new ObjectOutputStream(fstream);
			oos.writeObject(page);
			oos.close();
		} catch (IOException e) {
			System.err.println("Error Writing to file");
			e.printStackTrace(System.err);
		}
	}
	
	public void pushUp(int start, int maxRowsPerPage) {
		int end = start + 1;
		Page startPage = readPage(dbHelper.getPagePath(tableName, 0));
		while(start < pageCount && end < pageCount) {
			if(start == end) {
				end++;
				continue;
			}
			startPage = readPage(dbHelper.getPagePath(tableName, start));
			Page endPage = readPage(dbHelper.getPagePath(tableName, end));
			int startPageSize = startPage.getSize();
			int endPageSize = endPage.getSize();
			int needed = maxRowsPerPage - startPageSize;
			if(startPageSize < maxRowsPerPage) {
				if(endPageSize > 0) {
					int take = Math.min(endPageSize, needed);
					Stack<Record> stack = new Stack<>();
					while(!endPage.getPage().isEmpty())
						stack.push(endPage.getPage().remove(endPage.getSize() - 1));
					while(take-- > 0) {
						startPage.getPage().add(stack.pop());
						needed--;
					}
					if(needed == 0) {
						writePage(dbHelper.getPagePath(tableName, start), startPage);
						start++;
					}
					while(!stack.isEmpty())
						endPage.getPage().add(stack.pop());
				} else {
					end++;
				}
			} else {
				writePage(dbHelper.getPagePath(tableName, start), startPage);
				start++;
			}
		}
		if(start < pageCount && startPage.getSize() > 0)
			writePage(dbHelper.getPagePath(tableName, start++), startPage);
		
		while (start < pageCount) { // delete extra pages
			File file = new File(dbHelper.getPagePath(tableName, start++));
			file.delete();
		}
	}
	
	public void pushDown(int start, int maxRowsPerPage) {
		for(; start < pageCount; start++) {
			Page startPage = readPage(dbHelper.getPagePath(tableName, start));
			int startPageSize = startPage.getSize();
			if(startPageSize > maxRowsPerPage) {
				if(start < pageCount - 1) {
					Page nextPage = readPage(dbHelper.getPagePath(tableName, start + 1));
					nextPage.getPage().add(0, startPage.getPage().remove(startPage.getSize() - 1));
					writePage(dbHelper.getPagePath(tableName, start + 1), nextPage);
				} else {
					Page lastPage = new Page();
					lastPage.getPage().add(startPage.getPage().remove(startPage.getSize() - 1));
					writePage(dbHelper.getPagePath(tableName, start + 1), lastPage);
				}
			}
			writePage(dbHelper.getPagePath(tableName, start), startPage);
		}
	}

	
	public String getTableName() {
		return tableName;
	}

	public int getPageCount() {
		return this.pageCount;
	}

	public void incPageCount() {
		this.pageCount++;
	}

	public void setPageCount(int x) {
		this.pageCount = x;
	}
}
