package com.rt14.cop5725;

// IMPORTS
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

// Basic implementation of Pass Two/Group Two cleaning algorithms (deduplication, conflict resolution)
public class GroupTwo {
	private static double LD_THRESHOLD = 10.0; // levenshtein distance threshold
	private static ArrayList<String> clusters = new ArrayList<String>();
	private static Map<String, ArrayList<ArrayList<String>>> finalData = null;
	
	public static boolean run(Map<String, ArrayList<ArrayList<String>>> data1, Map<String, ArrayList<ArrayList<String>>> data2) {
		GroupTwo.Deduplication(data1, data2);
		GroupTwo.ConflictResolution(finalData);
		return true;
	}
	
	// remove exact duplicate tuples from input data
	private static boolean Deduplication(Map<String, ArrayList<ArrayList<String>>> data1, Map<String, ArrayList<ArrayList<String>>> data2) {
		// merge data1 and data2
		finalData = new HashMap<String, ArrayList<ArrayList<String>>>();
		finalData.putAll(data1);
		for(Map.Entry<String, ArrayList<ArrayList<String>>> e : data2.entrySet()) {
			finalData.merge(e.getKey(), e.getValue(), (value1, value2) -> { value1.addAll(value2); return value1; });
		}
		System.out.println("Data sources merged");
		
		// handle removal of exact duplicate tuples
		for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : finalData.entrySet()) {
			// key = entry.getKey(), value = entry.getValue()
			Set<ArrayList<String>> s = new LinkedHashSet<ArrayList<String>>(entry.getValue());
			ArrayList<ArrayList<String>> t = new ArrayList<ArrayList<String>>(s);
			finalData.replace(entry.getKey(), t);
		}
		System.out.println("Removed exact duplicates");
		
		// create clusters for fuzzy duplicate matching
		for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : finalData.entrySet()) {
			for(int i = 0; i < entry.getValue().size(); i++) {
				generateClusters(entry.getValue().get(i));
			}
		}
		// use clusters to remove fuzzy duplicates
		for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : data1.entrySet()) {
			for(int i = 0; i < entry.getValue().size(); i++) {
				for(int j = 0; j < clusters.size(); j++) {
					if(clusters.get(j).equals(entry.getValue().get(i).get(2))) {
						data1.get(entry.getKey()).get(i).set(2, data1.get(entry.getKey()).get(i).get(2));
					}
				}
			}
		}
		for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : data2.entrySet()) {
			for(int i = 0; i < entry.getValue().size(); i++) {
				for(int j = 0; j < clusters.size(); j++) {
					if(clusters.get(j).equals(entry.getValue().get(i).get(2))) {
						data2.get(entry.getKey()).get(i).set(2, data2.get(entry.getKey()).get(i).get(2));
					}
				}
			}
		}
		
		System.out.println("Deduplication finished");
		App.data1 = finalData;
		App.data2 = null;
		return true;
	}
	
	// generate similarity clusters for fuzzy matching
	private static void generateClusters(ArrayList<String> list) {
		LevenshteinDistance ld = new LevenshteinDistance();
		for(int i = 0; i < list.size(); i++) {
			for(int ii = i+1; ii < list.size(); ii++) {
				if(ld.distance(list.get(i), list.get(ii)) <= LD_THRESHOLD) {
					clusters.add(list.get(i));
					clusters.add(list.get(ii));
				}
			}
		}
	}
	
	// remove potential PRIMARY KEY conflicts by adding a new column/attribute to every tuple which
	// becomes the new PRIMARY KEY
	private static boolean ConflictResolution(Map<String, ArrayList<ArrayList<String>>> fd) {
		for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : fd.entrySet()) {
			int pk = 1;
			for(int i = 0; i < entry.getValue().size(); i++) {
				entry.getValue().get(i).add(0, String.valueOf(pk));
				pk++;
			}
		}
		App.data1 = fd;
		finalData = fd;
		return true;
	}
}
