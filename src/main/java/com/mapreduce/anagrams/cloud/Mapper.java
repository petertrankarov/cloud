package com.mapreduce.anagrams.cloud;

import java.util.ArrayList; // Import the Java ArrayList Class
import java.util.Arrays;

public class Mapper {
	
	public static ArrayList<String> words = new ArrayList<String>();
	
	public static ArrayList<String[]> map(Object[] in) {
		
		ArrayList<String[]> out = new ArrayList<String[]>();
		int size = in.length;
		
		for (int i = 0; i < size; i++) {
			
			/*
			 * Loop through words in ArrayList
			 * create char array and sort alphabetically
			 * create string of sorted word
			 * input key(sorted chars)-value(actual word) pair into array
			 * add to output ArrayList
			 */
			
			String word = in[i].toString();
			char[] wordChars = word.toCharArray();
			Arrays.sort(wordChars);
			String sortedWord = new String(wordChars);
			String[] keyValue = {sortedWord,word};
			out.add(keyValue);
			
		}
		
		// return new ArrayList containing key-value pairs
		return out;
		
	}
	
	public static void main(String[] args) throws Exception {
		
		/*
		 * Now the data from a book has been read, we want to map it (forming key value pairs)
		 * The program loops through each word in an input ArrayList
		 * It then splits the word into characters, and their significant character-count
		 * It also sorts these characters alphabetically and removes any duplicate values
		 * This way we're left with an array in the form of e.g.
		 * 		for word "hello" -> ["ehllo", "hello"]
		 */		
		
	}
	
	
}
