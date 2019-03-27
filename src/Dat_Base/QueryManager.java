package Dat_Base;

import java.util.Stack;

public class QueryManager {

	public QueryManager() {

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
