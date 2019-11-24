package com.rt14.cop5725;

// IMPORTS
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// Q-Gram string similarity class with K = 2 (Q2-Gram)
public class Q2Gram extends StringSimilarity {
	public Q2Gram(int k) {
		super(k);
	}
	
	public Q2Gram() {
		super();
	}
	
	// calculate string similarity based on distance between two strings
	public final double distance(String s1, String s2) {
		if(s1 == null) {
			throw new NullPointerException();
		}
		if(s2 == null) {
			throw new NullPointerException();
		}
		if(s1.equals(s2)) {
			return 0;
		}
		Map<String, Integer> p1 = getP(s1);
		Map<String, Integer> p2 = getP(s2);
		return distance(p1, p2);
	}
	
	// calculate string similarity based on pre-calculated distance profiles
	public final double distance(Map<String, Integer> p1, Map<String, Integer> p2) {
		Set<String> combine = new HashSet<String>();
		combine.addAll(p1.keySet());
		combine.addAll(p2.keySet());
		int result = 0;
		for(String key : combine) {
			int x = 0;
			int y = 0;
			Integer iv = p1.get(key);
			if(iv != null) {
				x = iv;
			}
			Integer iw = p2.get(key);
			if(iw != null) {
				y = iw;
			}
			result += Math.abs(x - y);
		}
		return result;
	}
}
