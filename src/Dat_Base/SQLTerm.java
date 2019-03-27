package Dat_Base;

public class SQLTerm {
	private String tableName;
	private String columnName;
	private String operator;
	private Object objValue;

	public SQLTerm(String tableName, String columnName, String operator, Object objValue) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.operator = operator;
		this.objValue = objValue;
	}

	@SuppressWarnings("unchecked")
	public boolean verfiyCondition(Record record) {
		Comparable<Object> value = (Comparable<Object>) record.getCell(columnName);
		switch (operator) {
		case ">":
			return value.compareTo(objValue) > 0;
		case ">=":
			return value.compareTo(objValue) >= 0;
		case "<":
			return value.compareTo(objValue) < 0;
		case "<=":
			return value.compareTo(objValue) <= 0;
		case "!=":
			return value.compareTo(objValue) != 0;
		case "=":
			return value.compareTo(objValue) == 0;
		default:
			return false;
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public Object getObjValue() {
		return objValue;
	}

	public void setObjValue(Object objValue) {
		this.objValue = objValue;
	}

}
