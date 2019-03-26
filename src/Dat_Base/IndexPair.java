package Dat_Base;

import java.io.Serializable;

public class IndexPair implements Comparable<IndexPair>, Serializable {

	private static final long serialVersionUID = 1L;
	private Comparable<Object> value;
	private String bits;

	public IndexPair(Comparable<Object> value, String bits) {
		this.value = value;
		this.bits = bits;
	}

	public IndexPair(Comparable<Object> value, int size) {
		this.value = value;
		StringBuilder res = new StringBuilder();
		for (int i = 0; i < size; i++)
			res.append('0');
		this.bits = res.toString();
	}

	public void set(int index) {
		bits = bits.substring(0, index) + "1" + bits.substring(index + 1, bits.length());
	}

	public void reset(int index) {
		bits = bits.substring(0, index) + "0" + bits.substring(index + 1, bits.length());
	}

	public void insert(int index, String bit) {
		System.out.println("Index: " + index + "String: " + bits);
		if(index == bits.length())
			bits += bit;
		else if(index == 0)
			bits = bit + bits;
		else
			bits = bits.substring(0, index) + bit + bits.substring(index, bits.length());
		System.out.println("After: " + bits);
	}

	public void delete(int index) {
		if(index == 0 && index == bits.length() - 1)
			bits = "";
		else if(index == bits.length() - 1)
			bits = bits.substring(0, bits.length() - 1);
		else if(index == 0)
			bits = bits.substring(1, bits.length());
		else
			bits = bits.substring(0, index) + bits.substring(index + 1, bits.length());
	}

	public static String or(String a, String b) {
		StringBuilder res = new StringBuilder();
		for (int i = 0; i < a.length(); i++)
			res.append(a.charAt(i) == '1' || b.charAt(i) == '1' ? '1' : '0');
		return res.toString();
	}

	public static String and(String a, String b) {
		StringBuilder res = new StringBuilder();
		for (int i = 0; i < a.length(); i++)
			res.append(a.charAt(i) == '1' && b.charAt(i) == '1' ? '1' : '0');
		return res.toString();
	}

	public static String xor(String a, String b) {
		StringBuilder res = new StringBuilder();
		for (int i = 0; i < a.length(); i++)
			res.append(a.charAt(i) != b.charAt(i) ? '1' : '0');
		return res.toString();
	}

	@Override
	public int compareTo(IndexPair o) {
		return value.compareTo(o.value);
	}

	public Comparable<Object> getValue() {
		return value;
	}

	public void setValue(Comparable<Object> value) {
		this.value = value;
	}

	public String getBits() {
		return bits;
	}

	public void setBits(String bits) {
		this.bits = bits;
	}

	@Override
	public String toString() {
		return "(" + value + " : " + bits + ")";
	}
}
