package com.rt14.cop5725;

// IMPORTS
import java.util.Map;

// Cosine similarity implementation
public class Cosine extends StringSimilarity {
	public Cosine(int k) {
		super(k);
	}
	
	public Cosine() {
		super();
	}
	
	// calculate similarity based on two strings
	public final double similarity(String s1, String s2) {
		if(s1 == null) {
			throw new NullPointerException();
		}
		if(s2 == null) {
			throw new NullPointerException();
		}
		if(s1.equals(s2)) {
			return 1;
		}
		if((s1.length() < getK()) || (s2.length() < getK())) {
			return 0;
		}
		Map<String, Integer> p1 = getP(s1);
		Map<String, Integer> p2 = getP(s2);
		return (dotProduct(p1, p2)) / (norm(p1) * norm(p2));
	}
	
	// calculate similarity based on pre-computed profiles
	public final double similarity(Map<String, Integer> p1, Map<String, Integer> p2) {
		return (dotProduct(p1, p2) / (norm(p1) * norm(p2)));
	}
	
	// normalize similarity between strings with a distance value <= 1.0
	public final double distance(String s1, String s2) {
		return (1.0 - similarity(s1, s2));
	}
	
	// normalize the profiles
	private static double norm(Map<String, Integer> p) {
		double result = 0;
		for(Map.Entry<String, Integer> entry : p.entrySet()) {
			result += 1.0 * entry.getValue() * entry.getValue();
		}
		return Math.sqrt(result);
	}
	
	// dot product implementation
	private static double dotProduct(Map<String, Integer> p1, Map<String, Integer> p2) {
		Map<String, Integer> sm = p2;
		Map<String, Integer> lg = p1;
		if(p1.size() < p2.size()) {
			sm = p1;
			lg = p2;
		}
		double result = 0;
		for(Map.Entry<String, Integer> entry : sm.entrySet()) {
			Integer i = lg.get(entry.getKey());
			if(i == null) {
				continue;
			}
			result += 1.0 * entry.getValue() * i;
		}
		return result;
	}
}
