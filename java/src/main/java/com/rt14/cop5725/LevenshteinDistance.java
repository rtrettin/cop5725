package com.rt14.cop5725;

// Implementation of the Levenshtein string distance algorithm
public class LevenshteinDistance {
	public double distance(String s1, String s2) {
		return distance(s1, s2, Integer.MAX_VALUE);
	}
	
	// calculate the distance between two strings according to Levenshtein
	public double distance(String s1, String s2, int limit) {
		if(s1 == null || s2 == null) {
			throw new NullPointerException();
		}
		
		if(s1.equals(s2)) {
			return 0;
		}
		
		if(s1.length() == 0 || s2.length() == 0) {
			return 0;
		}
		
		int[] wv1 = new int[s2.length() + 1];
		int[] wv2 = new int[s2.length() + 1];
		int[] wvtemp;
		
		for(int i = 0; i < wv1.length; i++) {
			wv1[i] = i;
		}
		for(int i = 0; i < s1.length(); i++) {
			wv2[0] = i + 1;
			int min = wv2[0];
			for(int j = 0; j < s2.length(); j++) {
				int cost = 1;
				if(s1.charAt(i) == s2.charAt(j)) {
					cost = 0;
				}
				wv2[j + 1] = Math.min(wv2[j] + 1, Math.min(wv1[j + 1] + 1, wv1[j] + cost));
				min = Math.min(min, wv2[j + 1]);
			}
			if(min >= limit) {
				return limit;
			}
			wvtemp = wv1;
			wv1 = wv2;
			wv2 = wvtemp;
		}
		return wv1[s2.length()];
	}
}
