package habal;


public class try1 {
	public static void main(String[] args) {
		System.out.println(encode("000000000000111111111111001010010010100000001100111000101010010101010101")); //511121112113
		System.out.println(decode("12,12,2,1,1,1,2,1,2,1,1,1,7,2,2,3,3,1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1")); 
		System.out.println(encode("1111010010100100100010100100001000000000011111111111")); //041121012121311121411011
		System.out.println(decode("0,4,1,1,2,1,1,1,2,1,2,1,3,1,1,1,2,1,4,1,10,11"));
	}														
public static String encode(String string) {
    if (string == null || string.isEmpty()) return "";

    StringBuilder builder = new StringBuilder();
    char[] chars = string.toCharArray();
    char current ='0';
    int count = 0;

    for (int i = 0; i < chars.length; i++) {
        if (current == chars[i]){
            count++;
        } else {
            builder.append(count+",");
            if(current=='0')
            	current = '1';
            else current='0';
            count = 1;
        }
    }
    builder.append(count);
    return builder.toString();
}
public static String decode(String string) {
    if (string == null || string.isEmpty()) return "";

    StringBuilder builder = new StringBuilder();
    String[] Array = string.split(",");
    for(int i=0;i<Array.length;i++) {
    	int repetitions=Integer.parseInt(Array[i]);
    	if(i%2==0) {
    		for(int j=0;j<repetitions;j++) {
    			builder.append("0");
    		}
    	}
    	else {
    		for(int j=0;j<repetitions;j++) {
    			builder.append("1");
    		}
    	}
        
    }
    return builder.toString();
}

}
