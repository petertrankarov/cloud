package com.mapreduce.anagrams.cloud;

import java.util.ArrayList;
import java.util.Comparator;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import com.google.api.gax.paging.Page;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class Reader {

	public static String projectId = ServiceOptions.getDefaultProjectId();
	public static String booksBucket = "coc105-gutenburg-5000books";
	public static String myBucket = "b919372-mapreduce-anagrams";
	public static String subscriptionId = "Anagram-Subscription";
	
	public static String[] stopWords = {"tis","twas","a","able","about","across","after","aint","all","almost","also","am","among","an","and","any","are"
			,"arent","as","at","be","because","been","but","by","can","cant","cannot","could","couldve","couldnt","dear","did","didnt","do","does","doesnt"
			,"dont","either","else","ever","every","for","from","get","got","had","has","hasnt","have","he","hed","hell","hes","her","hers","him","his"
			,"how","howd","howll","hows","however","i","id","ill","im","ive","if","in","into","is","isnt","it","its","its","just","least","let","like"
			,"likely","may","me","might","mightve","mightnt","most","must","mustve","mustnt","my","neither","no","nor","not","of","off","often","on","only"
			,"or","other","our","own","rather","said","say","says","shant","she","shed","shell","shes","should","shouldve","shouldnt","since","so","some"
			,"than","that","thatll","thats","the","their","them","then","there","theres","these","they","theyd","theyll","theyre","theyve","this","tis"
			,"to","too","twas","us","wants","was","wasnt","we","wed","well","were","were","werent","what","whatd","whats","when","when","whend","whenll"
			,"whens","where","whered","wherell","wheres","which","while","who","whod","wholl","whos","whom","why","whyd","whyll","whys","will","with"
			,"wont","would","wouldve","wouldnt","yet","you","youd","youll","youre","youve","your"};
	public static ArrayList<String> words = new ArrayList<String>();

	public static void uploadObjectFromMemory(String projectId, String bucketName, String objectName, String contents) throws IOException {

		/*
		 * This method is used to upload data to a file in the storage bucket
		 */

		Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		BlobId blobId = BlobId.of(bucketName, objectName);// get details of GCS file
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		byte[] content = contents.getBytes(StandardCharsets.UTF_8);// convert string to readable content
		storage.createFrom(blobInfo, new ByteArrayInputStream(content));// replace file in GCS with content

  	}

	public static void downloadObject(String projectId, String bucketName, String destFilePath) {

		/*
		 * This method is used to download all the books from the public repository to a local folder
		 */

		// loop through all items in gutenburg repository
		Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		Page<Blob> blobs = storage.list(bucketName);

		for (Blob blob : blobs.iterateAll()){	

			File file = new File(blob.getName().toString());
			if (!file.isFile()){// check if file already exists
				// download file to folder containing books
				blob.downloadTo(Paths.get(destFilePath+file.getName()));
			}

		} 

	}	

	public static void mergeBooks() throws IOException {

		/*
		 * This method merges all the book files previously downloaded into one file in order to process as a whole
		 */

		File dir = new File("books/");
		PrintWriter pw = new PrintWriter("alldata.txt");
		pw.flush();// clear contents of all data
		String[] filenames = dir.list();
		
		for (String filename : filenames) {// loop through files in book directory

			File f = new File(dir, filename);// open file
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();// read the line

			while (line != null){// write lines to all data

				pw.println(line);// add to alldata
				line = br.readLine();
			
			}

			br.close();
			pw.flush();// clear contents for next file

		}
		pw.close();
		System.out.println("Reading complete.");

	}
	
	public static ArrayList<String> read(String bookName) {
		
		String line;
		String[] lineWords;
		
		/*
		 * The method imports the raw contents from a book file,
		 * splits the data from each line into words in an array,
		 * the data is then filtered of punctuation and duplicate words,
		 * lastly any stop words included in the list are removed
		 */
		
		try {
			
			// try read input file
			File myObj = new File(bookName);
			Scanner reader = new Scanner(myObj);
			while (reader.hasNextLine()) {
				
				// split line of text into words
				line = reader.nextLine().toLowerCase();
				lineWords = line.split("\\W+");
				
				for (int i = 0; i < lineWords.length; i++) {
					// remove punctuation
					lineWords[i] = lineWords[i].replaceAll("[^a-zA-Z ]", "");
					// if word isn't already present, add to list of words
					if (words.indexOf(lineWords[i]) == -1) {
						words.add(lineWords[i]);
					}
				}
			}
			
			reader.close();
			
			// remove stop words from word list
			for (int j = 0; j < stopWords.length; j++) {
				if (words.contains(stopWords[j])) {
					words.remove(stopWords[j]);
				}
			}
			
		} catch (FileNotFoundException e) {
			
			// error catching if file not found			
			System.out.println("System cannot find your book.");
			e.printStackTrace();
			
		}
		
		return words;
		
	}
	
	/**
	 * @param words
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<Object[]> split(ArrayList<String> words) throws IOException {

		/*
		 * This method splits the total arraylist into smaller arraylists with equal length words,
		 * since they don't need to be processed together, this will allow us to run numerous
		 * mappers and reducers at once to speed up the time to get results
		 */
		
		int len = words.stream().max(Comparator.comparingInt(String::length)).get().length();
		ArrayList<Object[]> wordsPart = new ArrayList<Object[]>();
		ArrayList<String> temp = new ArrayList<String>();

		for (int i = 1; i <= len; i++) {// for each different word length

			temp.clear();
			int j = 0;// start at beginning of word list

			while (!words.isEmpty()) {// loop through words while not empty

				String word = words.get(j);// get the next word
				int max = words.size()-1;// get the element id of the last word

				if (j != max) {// if not at end of list

					if (word.length() == i) {// if it has length i
						temp.add(word);// add to temp list
						words.remove(j);// remove from big list
					} else {// check next word
						j++;
					}

				} else {// reached end of list, break while
					break;
				}
			}

			System.out.println("Sorted words of size: "+ i);
			//uploadObjectFromMemory(projectId, myBucket, "splitWords/words"+Integer.toString(i)+".txt", temp.toString());
			Object[] tempArr = temp.toArray();
			wordsPart.add(tempArr);// return sublist of words with length i
		}

		for (Object[] word : wordsPart) {
			new Thread() {
				Object[] wordpart = word;
				public void run() {
					//System.out.println(wordpart[0]);
					try {
						Reducer.reduce(Mapper.map(word));
						System.out.println("Map Reduce complete for words with character length: " + Integer.toString(wordpart[0].toString().length()));
					} catch (IOException e) {
						e.printStackTrace();
						System.out.println("Map Reduce failed for words with character length: " + Integer.toString(wordpart[0].toString().length()));
					} catch (ExecutionException e) {
						e.printStackTrace();
						System.out.println("Map Reduce failed for words with character length: " + Integer.toString(wordpart[0].toString().length()));
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.out.println("Map Reduce failed for words with character length: " + Integer.toString(wordpart[0].toString().length()));
					}
					
				}
			}.start();
		}

		return wordsPart;
		
	}
	
	public static void main(String[] args) throws Exception {
		
		/*
		 * The first step is to read the data from a book
		 * The raw data needs to be split into words
		 * The split method then divides the words into 5 smaller lists
		 */
		
		/*downloadObject(projectId, bucketName, "books/");
		mergeBooks();
		read("alldata.txt");
		split(words);*/

		downloadObject(projectId, booksBucket, "books/");
		mergeBooks();
		split(read("alldata.txt"));
		PubSubMessages.subscribe(projectId, subscriptionId);

	}
	
	
}
