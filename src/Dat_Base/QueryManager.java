package Dat_Base;

import java.util.Stack;

public class QueryManager {
	
	private DBHelper dbHelper;

	public QueryManager(DBHelper dbHelper) {
		this.dbHelper = dbHelper;
	}
	
	@SuppressWarnings("unchecked")
	public PositionPair searchTablePages(String tableName, Object target) {
		Comparable<Object> targetKey = (Comparable<Object>)target;
		Table table = new Table(tableName, dbHelper);
		//Binary Search on the Pages of the table
		int loPage = 0, hiPage = table.getPageCount() - 1, midPage, chosenPage = -1;
		while(loPage <= hiPage) {
			midPage = loPage + (hiPage - loPage) / 2;
			Page currPage = table.readPage(dbHelper.getPagePath(tableName, midPage));
			Comparable<Object> lastRecord = (Comparable<Object>)currPage.getRecord(currPage.getSize() - 1).getPrimaryKey();
			if(targetKey.compareTo(lastRecord) <= 0) {
				chosenPage = midPage;
				hiPage = midPage - 1;
			} else {
				loPage = midPage + 1;
			}
		}
		if(chosenPage == -1) {
			if(table.getPageCount() == 0)
				return new PositionPair(0, 0);
			int lastPageIndex = table.getPageCount() - 1;
			int newLastRecordIndex = table.readPage(dbHelper.getPagePath(tableName, lastPageIndex)).getSize();
			return new PositionPair(lastPageIndex, newLastRecordIndex);
		}
		//Binary Search on the records of the chosenPage
		Page searchPage = table.readPage(dbHelper.getPagePath(tableName, chosenPage));
		int loRecord = 0, hiRecord = searchPage.getSize() - 1, midRecordIndex, chosenRecord = -1;
		while(loRecord <= hiRecord) {
			midRecordIndex = loRecord + (hiRecord - loRecord) / 2;
			Comparable<Object> midRecord = (Comparable<Object>)searchPage.getRecord(midRecordIndex).getPrimaryKey();
			if(targetKey.compareTo(midRecord) <= 0) {
				chosenRecord = midRecordIndex;
				hiRecord = midRecordIndex - 1;
			} else {
				loRecord = midRecordIndex + 1;
			}
		}
		return new PositionPair(chosenPage, chosenRecord);
	}
	
	@SuppressWarnings("unchecked")
	public PositionPair searchIndexPages(String tableName, String indexedCol, Object target) {
		Comparable<Object> targetValue = (Comparable<Object>)target;
		BitMap bitMap = new BitMap(tableName, indexedCol, dbHelper);
		//Binary Search on the Pages of the table
		int loPage = 0, hiPage = bitMap.getPageCount() - 1, midPage, chosenPage = -1;
		while(loPage <= hiPage) {
			midPage = loPage + (hiPage - loPage) / 2;
			IndexPage currPage = bitMap.readPage(dbHelper.getIndexPagePath(tableName, indexedCol, midPage));
			Comparable<Object> lastRecord = currPage.getIndexPair(currPage.getSize() - 1).getValue();
			if(targetValue.compareTo(lastRecord) <= 0) {
				chosenPage = midPage;
				hiPage = midPage - 1;
			} else {
				loPage = midPage + 1;
			}
		}
		if(chosenPage == -1) {
			if(bitMap.getPageCount() == 0)
				return new PositionPair(0, 0);
			int lastPageIndex = bitMap.getPageCount() - 1;
			int newLastRecordIndex = bitMap.readPage(dbHelper.getIndexPagePath(tableName, indexedCol, lastPageIndex)).getSize();
			return new PositionPair(lastPageIndex, newLastRecordIndex);
		}
		//Binary Search on the records of the chosenPage
		IndexPage searchPage = bitMap.readPage(dbHelper.getIndexPagePath(tableName, indexedCol, chosenPage));
		int loValue = 0, hiValue = searchPage.getSize() - 1, midValueIndex, chosenValue = -1;
		while(loValue <= hiValue) {
			midValueIndex = loValue + (hiValue - loValue) / 2;
			Comparable<Object> midValue = (Comparable<Object>)searchPage.getIndexPair(midValueIndex).getValue();
			if(targetValue.compareTo(midValue) <= 0) {
				chosenValue = midValueIndex;
				hiValue = midValueIndex - 1;
			} else {
				loValue = midValueIndex + 1;
			}
		}
		return new PositionPair(chosenPage, chosenValue);
	}

	public int getPrecedence(String operator) {
		switch (operator) {
		case "AND":
			return 3;
		case "XOR":
			return 2;
		case "OR":
			return 1;
		default:
			return 0;
		}
	}
	
	public boolean calcOperation(String operator, boolean a, boolean b) {
		switch (operator) {
		case "AND":
			return a && b;
		case "XOR":
			return a != b;
		case "OR":
			return a || b;
		default:
			return false;
		}
	}

	public boolean verfiyWhereClause(SQLTerm[] sqlTerms, String[] operators, Record record) throws DBAppException {
		if (operators.length < sqlTerms.length - 1)
			throw new DBAppException("Invalid Where Clause");
		int i = 0, j = 0;
		Stack<String> operatorsStack = new Stack<>();
		Stack<Boolean> result = new Stack<>();
		boolean turn = true;
		while (i < sqlTerms.length) {
			if (turn) {
				result.push(sqlTerms[i++].verfiyCondition(record));
			} else {
				while (!operatorsStack.isEmpty()
						&& getPrecedence(operatorsStack.peek()) > getPrecedence(operators[j])) {
					result.push(calcOperation(operatorsStack.pop(), result.pop(), result.pop()));
				}
				operatorsStack.push(operators[j++]);
			}
			turn = !turn;
		}
		while(!operatorsStack.isEmpty())
			result.push(calcOperation(operatorsStack.pop(), result.pop(), result.pop()));
		return result.pop();
	}
}
