package com.rt14.cop5725;

// IMPORTS
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

// Base abstract class for string similarity children classes
public abstract class StringSimilarity {
	private static final int K = 3;
	private int k = 0;
	private static final Pattern SPACE = Pattern.compile("\\s+");
	
	public StringSimilarity(int k) {
		if(k <= 0) {
			throw new IllegalArgumentException();
		}
		this.k = k;
	}
	
	StringSimilarity() {
		this(K);
	}
	
	public final int getK() {
		return k;
	}
	
	// calculate the similarity profile for a given string
	public final Map<String, Integer> getP(String string) {
		HashMap<String, Integer> hm = new HashMap<String, Integer>();
		String repl = SPACE.matcher(string).replaceAll(" ");
		for(int i = 0; i < (repl.length() - k + 1); i++) {
			String sub = repl.substring(i, i + k);
			Integer old = hm.get(sub);
			if(old != null) {
				hm.put(sub, old + 1);
			}else{
				hm.put(sub, 1);
			}
		}
		return Collections.unmodifiableMap(hm);
	}
}
