package com.rt14.cop5725;

// IMPORTS
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

// Implement Pass One/Group One algorithms (abnormal value detection/correction, incomplete data filling)
public class GroupOne {
	private static double IDF_THRESHOLD = 20.0; // incomplete data filling/QGram matching threshold
	private static ArrayList<Map<String, Integer>> PRECOMPUTED_PROFILE = new ArrayList<Map<String, Integer>>();
	private static Q2Gram q2 = new Q2Gram(2);
	
	public static boolean run(Map<String, ArrayList<ArrayList<String>>> data1, Map<String, ArrayList<ArrayList<String>>> data2, JSONObject rules1, JSONObject rules2) throws JSONException, ParseException {
		GroupOne.AVDC(data1, data2, rules1, rules2);
		return true;
	}
	
	@SuppressWarnings({ "unused", "deprecation" })
	private static boolean AVDC(Map<String, ArrayList<ArrayList<String>>> data1, Map<String, ArrayList<ArrayList<String>>> data2, JSONObject rules1, JSONObject rules2) throws JSONException, ParseException {
		System.out.println("AVDC source 1 started");
		for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : data1.entrySet()) {
			// key = entry.getKey(), value = entry.getValue()
			JSONObject rule_subset = rules1.getJSONObject(entry.getKey());
			for(int i = 0; i < entry.getValue().size(); i++) {
				for(int j = 0; j < entry.getValue().get(i).size(); j++) {
					String d = entry.getValue().get(i).get(j); // individual data value as a String
					// these rules always exist
					String filling_rule = rule_subset.getJSONObject(String.valueOf(j)).getString("filling_rule"); // individual rule value applying to the current data value
					String column_name = rule_subset.getJSONObject(String.valueOf(j)).getString("column_name");
					String can_be_empty = rule_subset.getJSONObject(String.valueOf(j)).getString("can_be_empty");
					String intelligent = rule_subset.getJSONObject(String.valueOf(j)).getString("intelligent");
					String need_to_check = rule_subset.getJSONObject(String.valueOf(j)).getString("need_to_check");
					String data_type = rule_subset.getJSONObject(String.valueOf(j)).getString("data_type");
					// these rules sometimes exist based on data_type
					String cr_min = null;
					String cr_max = null;
					String cr_regex = null;
					String cr_early = null;
					String cr_late = null;
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_min")) {
						cr_min = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_min");
					}
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_max")) {
						cr_max = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_max");
					}
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_regex")) {
						cr_regex = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_regex");
					}
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_early")) {
						cr_early = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_early");
					}
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_late")) {
						cr_late = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_late");
					}
					
					// detection and correction processing
					if(need_to_check.equals("yes")) {
						if(data_type.contains("float")) { // FLOATs
							Float temp = Float.valueOf(d);
							if((temp < Float.valueOf(cr_min) || temp > Float.valueOf(cr_max))) {
								if(can_be_empty.equals("yes")) {
									data1.get(entry.getKey()).get(i).set(j, "");
								}else{
									if(intelligent.equals("yes")) {
										int MAX_ITER = 99;
										int ITER = 0;
										if(temp < Float.valueOf(cr_min)) {
											temp = -temp;
										}
										while(temp > Float.valueOf(cr_max) && ITER <= MAX_ITER) {
											temp -= 10;
											ITER += 1;
										}
										if(ITER >= MAX_ITER && temp > Float.valueOf(cr_max)) {
											temp = getListAvg(data1.get(entry.getKey()), j);
										}
										data1.get(entry.getKey()).get(i).set(j, String.valueOf(temp));
									}else{
										data1.get(entry.getKey()).get(i).set(j, filling_rule);
									}
								}
							}
						}else if(data_type.contains("int")) { // INTs
							Integer temp = Integer.valueOf(d);
							if((temp < Integer.valueOf(cr_min) || temp > Integer.valueOf(cr_max))) {
								if(can_be_empty.equals("yes")) {
									data1.get(entry.getKey()).get(i).set(j, "");
								}else{
									if(intelligent.equals("yes")) {
										int MAX_ITER = 99;
										int ITER = 0;
										if(temp < Integer.valueOf(cr_min)) {
											temp = getLastValueOf(data1, j, entry.getKey()) + 1;
										}
										while(temp > Integer.valueOf(cr_max) && ITER <= MAX_ITER) {
											temp -= 10;
											ITER += 1;
										}
										if(ITER >= MAX_ITER && temp > Integer.valueOf(cr_max)) {
											temp = Math.round(getListAvg(data1.get(entry.getKey()), j));
										}
										data1.get(entry.getKey()).get(i).set(j, String.valueOf(temp));
									}else{
										data1.get(entry.getKey()).get(i).set(j, filling_rule);
									}
								}
							}
						}else if(data_type.contains("datetime")) { // DATETIMEs
							cr_early = cr_early.replace('T', ' ').concat(":00");
							cr_late = cr_late.replace('T', ' ').concat(":00");
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
							Date crearly = sdf.parse(cr_early);
							Date crlate = sdf.parse(cr_late);
							Date date = sdf.parse(d);
							if((date.compareTo(crearly) < 0 || date.compareTo(crlate) > 0)) {
								if(can_be_empty.equals("yes")) {
									data1.get(entry.getKey()).get(i).set(j, "");
								}else{
									if(intelligent.equals("yes")) {
										while(date.compareTo(crearly) < 0) {
											int current_year = date.getYear();
											date.setYear(current_year + 1);
										}
										while(date.compareTo(crlate) > 0) {
											int current_year = date.getYear();
											date.setYear(current_year - 1);
										}
										data1.get(entry.getKey()).get(i).set(j, sdf.format(date));
									}else{
										data1.get(entry.getKey()).get(i).set(j, filling_rule);
									}
								}
							}
						}else if(data_type.contains("varchar")) { // STRINGs
							Pattern p = Pattern.compile(cr_regex);
							Matcher m = p.matcher(d);
							if(!m.find()) {
								if(can_be_empty.equals("yes")) {
									data1.get(entry.getKey()).get(i).set(j, "");
								}else{
									if(intelligent.equals("yes")) {
										if(d.contains("-") && d.contains("x")) {
											data1.get(entry.getKey()).get(i).set(j, d.split("x")[0]);
										}else{
											String temp = getMostFrequent(data1.get(entry.getKey()), j);
											data1.get(entry.getKey()).get(i).set(j, temp);
										}
									}else{
										data1.get(entry.getKey()).get(i).set(j, filling_rule);
									}
								}
							}
						}
					}
					
					// Incomplete Data Filling stage:
					// only do it if the value is empty but is not allowed to be based on the given rule set
					if(need_to_check.equals("yes") && can_be_empty.equals("no") && data1.get(entry.getKey()).get(i).get(j).equals("")) {
						ArrayList<Integer> idf = GroupOne.IncompleteDataFilling();
						if(!idf.isEmpty()) {
							Random r = new Random();
							int rindex = r.nextInt(idf.size());
							data1.get(entry.getKey()).get(i).set(j, data1.get(entry.getKey()).get(rindex).get(j));
						}
					}
					// Calculate similarity profile of the tuple
					Map<String, Integer> profile = q2.getP(data1.get(entry.getKey()).get(i).get(j));
					PRECOMPUTED_PROFILE.add(profile);
				}
			}
		}
		System.out.println("AVDC source 1 ended");
		System.out.println("AVDC source 2 started");
		for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : data2.entrySet()) {
			// key = entry.getKey(), value = entry.getValue()
			JSONObject rule_subset = rules2.getJSONObject(entry.getKey());
			for(int i = 0; i < entry.getValue().size(); i++) {
				for(int j = 0; j < entry.getValue().get(i).size(); j++) {
					String d = entry.getValue().get(i).get(j); // individual data value as a String
					// these rules always exist
					String filling_rule = rule_subset.getJSONObject(String.valueOf(j)).getString("filling_rule"); // individual rule value applying to the current data value
					String column_name = rule_subset.getJSONObject(String.valueOf(j)).getString("column_name");
					String can_be_empty = rule_subset.getJSONObject(String.valueOf(j)).getString("can_be_empty");
					String intelligent = rule_subset.getJSONObject(String.valueOf(j)).getString("intelligent");
					String need_to_check = rule_subset.getJSONObject(String.valueOf(j)).getString("need_to_check");
					String data_type = rule_subset.getJSONObject(String.valueOf(j)).getString("data_type");
					// these rules sometimes exist based on data_type
					String cr_min = null;
					String cr_max = null;
					String cr_regex = null;
					String cr_early = null;
					String cr_late = null;
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_min")) {
						cr_min = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_min");
					}
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_max")) {
						cr_max = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_max");
					}
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_regex")) {
						cr_regex = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_regex");
					}
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_early")) {
						cr_early = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_early");
					}
					if(rule_subset.getJSONObject(String.valueOf(j)).has("cr_late")) {
						cr_late = rule_subset.getJSONObject(String.valueOf(j)).getString("cr_late");
					}
					
					// correction and detection processing
					if(need_to_check.equals("yes")) {
						if(data_type.contains("float")) { // FLOATs
							Float temp = Float.valueOf(d);
							if((temp < Float.valueOf(cr_min) || temp > Float.valueOf(cr_max))) {
								if(can_be_empty.equals("yes")) {
									data2.get(entry.getKey()).get(i).set(j, "");
								}else{
									if(intelligent.equals("yes")) {
										int MAX_ITER = 99;
										int ITER = 0;
										if(temp < Float.valueOf(cr_min)) {
											temp = -temp;
										}
										while(temp > Float.valueOf(cr_max) && ITER <= MAX_ITER) {
											temp -= 10;
											ITER += 1;
										}
										if(ITER >= MAX_ITER && temp > Float.valueOf(cr_max)) {
											temp = getListAvg(data2.get(entry.getKey()), j);
										}
										data2.get(entry.getKey()).get(i).set(j, String.valueOf(temp));
									}else{
										data2.get(entry.getKey()).get(i).set(j, filling_rule);
									}
								}
							}
						}else if(data_type.contains("int")) { // INTs
							Integer temp = Integer.valueOf(d);
							if((temp < Integer.valueOf(cr_min) || temp > Integer.valueOf(cr_max))) {
								if(can_be_empty.equals("yes")) {
									data2.get(entry.getKey()).get(i).set(j, "");
								}else{
									if(intelligent.equals("yes")) {
										int MAX_ITER = 99;
										int ITER = 0;
										if(temp < Integer.valueOf(cr_min)) {
											temp = getLastValueOf(data2, j, entry.getKey()) + 1;
										}
										while(temp > Integer.valueOf(cr_max) && ITER <= MAX_ITER) {
											temp -= 10;
											ITER += 1;
										}
										if(ITER >= MAX_ITER && temp > Integer.valueOf(cr_max)) {
											temp = Math.round(getListAvg(data2.get(entry.getKey()), j));
										}
										data2.get(entry.getKey()).get(i).set(j, String.valueOf(temp));
									}else{
										data2.get(entry.getKey()).get(i).set(j, filling_rule);
									}
								}
							}
						}else if(data_type.contains("datetime")) { // DATETIMEs
							cr_early = cr_early.replace('T', ' ').concat(":00");
							cr_late = cr_late.replace('T', ' ').concat(":00");
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
							Date crearly = sdf.parse(cr_early);
							Date crlate = sdf.parse(cr_late);
							Date date = sdf.parse(d);
							if((date.compareTo(crearly) < 0 || date.compareTo(crlate) > 0)) {
								if(can_be_empty.equals("yes")) {
									data2.get(entry.getKey()).get(i).set(j, "");
								}else{
									if(intelligent.equals("yes")) {
										while(date.compareTo(crearly) < 0) {
											int current_year = date.getYear();
											date.setYear(current_year + 1);
										}
										while(date.compareTo(crlate) > 0) {
											int current_year = date.getYear();
											date.setYear(current_year - 1);
										}
										data2.get(entry.getKey()).get(i).set(j, sdf.format(date));
									}else{
										data2.get(entry.getKey()).get(i).set(j, filling_rule);
									}
								}
							}
						}else if(data_type.contains("varchar")) { // STRINGs
							Pattern p = Pattern.compile(cr_regex);
							Matcher m = p.matcher(d);
							if(!m.find()) {
								if(can_be_empty.equals("yes")) {
									data2.get(entry.getKey()).get(i).set(j, "");
								}else{
									if(intelligent.equals("yes")) {
										if(d.contains("-") && d.contains("x")) {
											data2.get(entry.getKey()).get(i).set(j, d.split("x")[0]);
										}else{
											String temp = getMostFrequent(data2.get(entry.getKey()), j);
											data2.get(entry.getKey()).get(i).set(j, temp);
										}
									}else{
										data2.get(entry.getKey()).get(i).set(j, filling_rule);
									}
								}
							}
						}
					}
					
					// Incomplete data filling stage again, but for source 2
					if(need_to_check.equals("yes") && can_be_empty.equals("no") && data2.get(entry.getKey()).get(i).get(j).equals("")) {
						ArrayList<Integer> idf = GroupOne.IncompleteDataFilling();
						if(!idf.isEmpty()) {
							Random r = new Random();
							int rindex = r.nextInt(idf.size());
							data2.get(entry.getKey()).get(i).set(j, data2.get(entry.getKey()).get(rindex).get(j));
						}
					}
					Map<String, Integer> profile = q2.getP(data2.get(entry.getKey()).get(i).get(j));
					PRECOMPUTED_PROFILE.add(profile);
				}
			}
		}
		System.out.println("AVDC source 2 ended");
		App.data1 = data1;
		App.data2 = data2;
		return true;
	}
	
	// calculate and return the most frequent value that appears in
	// an arraylist object
	private static <T> T getMostFrequent(ArrayList<ArrayList<T>> arrayList, int j) {
		Map<T, Integer> map = new HashMap<>();
		for(int i = 0; i < arrayList.size(); i++) {
			Integer val = map.get(arrayList.get(i).get(j));
			map.put(arrayList.get(i).get(j), val == null ? 1 : val + 1);
		}
		Entry<T, Integer> max = null;
		for(Entry<T, Integer> e : map.entrySet()) {
			if(max == null || e.getValue() > max.getValue()) {
				max = e;
			}
		}
		return max == null ? null : max.getKey();
	}

	// calculate and return the mean average of the values in an
	// arraylist object
	private static Float getListAvg(ArrayList<ArrayList<String>> arrayList, int j) {
		Float sum = (float) 0.0;
		for(int i = 0; i < arrayList.size(); i++) {
			Float temp = Float.valueOf(arrayList.get(i).get(j));
			sum = sum + temp;
		}
		return (sum / (float)arrayList.size());
	}

	// Find similar data values based on the precomputed similarity profiles of previously processed tuples
	private static ArrayList<Integer> IncompleteDataFilling() {
		ArrayList<Integer> kinda = new ArrayList<Integer>();
		if(!(PRECOMPUTED_PROFILE.isEmpty() && PRECOMPUTED_PROFILE.size() <= 1)) {
			for(int c = 0; c < PRECOMPUTED_PROFILE.size(); c++) {
				for(int cc = c+1; cc < PRECOMPUTED_PROFILE.size(); cc++) {
					if(q2.distance(PRECOMPUTED_PROFILE.get(c), PRECOMPUTED_PROFILE.get(cc)) <= IDF_THRESHOLD) {
						kinda.add(c);
						kinda.add(cc);
					}
				}
			}
		}
		return kinda;
	}
	
	// Retrieve the last value of the given Map structure
	private static int getLastValueOf(Map<String, ArrayList<ArrayList<String>>> data, int location, String key) {
		int size1 = data.get(key).size() - 1;
		String result = data.get(key).get(size1).get(location);
		return Integer.valueOf(result);
	}
}
