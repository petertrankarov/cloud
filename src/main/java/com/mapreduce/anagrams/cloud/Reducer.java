package com.mapreduce.anagrams.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import com.google.cloud.ServiceOptions;

public class Reducer {

	public static String projectId = ServiceOptions.getDefaultProjectId();
	public static String myBucket = "b919372-mapreduce-anagrams";
	public static String topicId = "MapReduce-Anagrams";

	public static String reduce(ArrayList<String[]> in) throws IOException, ExecutionException, InterruptedException {
		
		/*
		 * This method adds the keys formed in the map stage to a new ArrayList
		 * The keys are then sorted and a temp array list is defined to store sets of anagrams
		 * The sorted key list is cleansed of any duplicates, then searched
		 * for every key in the list, any word matching the key is stored in temp
		 * If there are more than one elements in temp, 2+ matching words have been found (anagrams)
		 */

		String output = "List of anagrams - ";
		ArrayList<String> keys = new ArrayList<>();

		for (String[] pair : in) {
			if (keys.indexOf(pair[0]) == -1) {
				keys.add(pair[0]);
			}
		}
		Collections.sort(keys);
		ArrayList<String> temp = new ArrayList<>();
		int size = keys.get(0).length();

		for (String key : keys) {
			temp.clear();
			for (String[] pairs : in) {
				if (pairs[0].equals(key)) {
					temp.add(pairs[1]);
				}
			}
			if (temp.size() > 1) {
				output = output + temp.toString() + " | ";
			}
		}

		Reader.uploadObjectFromMemory(projectId, myBucket, "anagrams/"+Integer.toString(size)+".txt", output);
		PubSubMessages.publish(projectId, topicId, output);
		return output;
		
	}
	
	public static void main(String[] args) {
		
		/*
		 * The Mapper class has now formed key:value pairs in the form of string arrays
		 * These need to be compared in order to find anagrams
		 */

	}

}
