package com.rt14.cop5725;

import java.io.IOException;
// IMPORTS
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import io.socket.client.IO;
import io.socket.client.Manager;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;
import io.socket.engineio.client.Transport;
import okhttp3.OkHttpClient;
import okhttp3.Response;

import com.rt14.cop5725.GroupOne;
import com.rt14.cop5725.GroupTwo;
import com.rt14.cop5725.MySQLDB;

// Main runtime class
public class App {
	public static Map<String, ArrayList<ArrayList<String>>> data1 = new HashMap<String, ArrayList<ArrayList<String>>>(); // data structure for source 1 input
	public static Map<String, ArrayList<ArrayList<String>>> data2 = new HashMap<String, ArrayList<ArrayList<String>>>(); // data structure for source 2 input
	private static JSONObject rules1 = null; // JSON object for source 1 rules from the web interface
	private static JSONObject rules2 = null; // JSON object for source 2 rules from the web interface
	private static int stage = 0; // counter to keep track of the processing stage
	private static String ADB_URL = "http://";
	private static Map<String, ArrayList<String>> cnames = new HashMap<String, ArrayList<String>>(); // hold column names from input data sources (MySQL here)
	
	public static String genRandString(int length) {
		// generates a random string of given length
	    int leftLimit = 97; // 'a'
	    int rightLimit = 122; // 'z'
	    int targetStringLength = length;
	    Random random = new Random();
	    StringBuilder buffer = new StringBuilder(targetStringLength);
	    for (int i = 0; i < targetStringLength; i++) {
	        int randomLimitedInt = leftLimit + (int) 
	          (random.nextFloat() * (rightLimit - leftLimit + 1));
	        buffer.append((char) randomLimitedInt);
	    }
	    String generatedString = buffer.toString();
	    return generatedString;
	}
	
	// execute data cleaning tasks
    private static void proc() throws JSONException, ParseException, IOException {
    	System.out.println("Starting pass one/group one tasks (AVDC, IDF)");
        GroupOne.run(data1, data2, rules1, rules2);
        System.out.println("Starting pass two/group two tasks (DD, CR)");
        GroupTwo.run(data1, data2);
        System.out.println("Data cleaning tasks finished");
        
        // INSERT cleaned data into AsterixDB cluster
        System.out.println("Using " + App.ADB_URL);
        ADBClient adbc = new ADBClient(App.ADB_URL);
        adbc.post("/query/service", "DROP DATAVERSE cleanixingest IF EXISTS;CREATE DATAVERSE cleanixingest;"); // create 'database'
        System.out.println("[AsterixDB] Created cleanixingest dataverse");
        for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : App.data1.entrySet()) {
        	JSONObject meta = App.rules1.getJSONObject(entry.getKey());
        	String schema = "";
        	String primary_key = "";
        	ArrayList<String> tlist = new ArrayList<String>();
        	for(int i = -1; i < entry.getValue().get(0).size() - 1; i++) {
        		if(i == -1) {
        			primary_key = "asxid";
        			schema += "asxid: string,";
        			tlist.add("asxid");
        			continue;
        		}
        		String data_type = meta.getJSONObject(String.valueOf(i)).getString("data_type");
        		if(data_type.equals("int(11)")) {
        			data_type = "int";
        		}else if(data_type.equals("float")) {
        			data_type = "float";
        		}else if(data_type.equals("datetime")) {
        			data_type = "datetime";
        		}else{
        			data_type = "string";
        		}
        		if(i == entry.getValue().get(0).size() - 2) {
        			schema += meta.getJSONObject(String.valueOf(i)).getString("column_name") + ": string";
        		}else{
        			schema += meta.getJSONObject(String.valueOf(i)).getString("column_name") + ": string,";
        		}
        		tlist.add(meta.getJSONObject(String.valueOf(i)).getString("column_name"));
        	}
        	cnames.put(entry.getKey(), tlist);
        	// create 'tables'
        	adbc.post("/query/service", "USE cleanixingest;CREATE TYPE "+entry.getKey()+"Type AS closed {"+schema+"};CREATE DATASET "+entry.getKey()+"("+entry.getKey()+"Type) PRIMARY KEY "+primary_key+";");
        }
        System.out.println("[AsterixDB] Created tables in cleanixingest");
        for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : App.data1.entrySet()) {
        	for(int i = 0; i < entry.getValue().size(); i++) {
        		String insertion = "";
        		for(int j = 0; j < entry.getValue().get(i).size(); j++) {
        			String c = cnames.get(entry.getKey()).get(j);
        			if(j == entry.getValue().get(i).size() - 1) {
        				insertion += "\""+c+"\":\""+entry.getValue().get(i).get(j).replace("\n", " ")+"\"";
        			}else{
        				insertion += "\""+c+"\":\""+entry.getValue().get(i).get(j).replace("\n", " ")+"\",";
        			}
        		}
        		// create individual tuples
        		System.out.println("USE cleanixingest;INSERT INTO "+entry.getKey()+" ([{"+insertion+"}]);");
        		Response resp = adbc.post("/query/service", "USE cleanixingest;INSERT INTO "+entry.getKey()+" ([{"+insertion+"}]);");
        		System.out.println(resp);
        	}
        }
        System.out.println("[AsterixDB] Inserted all tuples into cleanixingest dataverse");
    }
	
	public static void main(String[] args) throws URISyntaxException {
		// setup and connect to SSL socket.io server
        OkHttpClient okHttpClient = new OkHttpClient.Builder().build();
        IO.setDefaultOkHttpWebSocketFactory(okHttpClient);
        IO.setDefaultOkHttpCallFactory(okHttpClient);
        IO.Options opts = new IO.Options();
        opts.callFactory = okHttpClient;
        opts.webSocketFactory = okHttpClient;
        opts.secure = true;
        final Socket socket = IO.socket("https://crustycrab.proxcp.com:8000", opts);
        
        // modify this socket.io client connection's HTTP Referer header so the server
        // can restrict origins safely
        socket.io().on(Manager.EVENT_TRANSPORT, new Emitter.Listener() {
        	@Override
        	public void call(Object... args) {
        		Transport transport = (Transport)args[0];
        		transport.on(Transport.EVENT_REQUEST_HEADERS, new Emitter.Listener() {
        			@Override
        			public void call(Object... args) {
        				@SuppressWarnings("unchecked")
        				Map<String, List<String>> headers = (Map<String, List<String>>)args[0];
        				headers.put("Referer", Arrays.asList("https://199.15.250.44:4433"));
        			}
        		});
        	}
        });
        // connect to the socket and start listening for messages
        socket.connect();
        
        // socket behavior definitions
        socket.on(Socket.EVENT_CONNECT, new Emitter.Listener() {
        	@Override
        	public void call(Object... args) {
        		System.out.println("Connected to socket");
        		socket.emit("addUserConnection", genRandString(12));
        	}
        });
        socket.on(Socket.EVENT_RECONNECTING, new Emitter.Listener() {
        	@Override
        	public void call(Object... args) {
        		System.out.println("Lost connection to socket! Attempting to reconnect...");
        	}
        });
        socket.on("Java_ConnectHyracksReq", new Emitter.Listener() {
        	@Override
        	public void call(Object... args) {
        		JSONObject obj = (JSONObject)args[0];
        		try {
        			App.ADB_URL += obj.getString("ccip") + ":" + obj.getString("ccp");
				} catch (JSONException e) {
					e.printStackTrace();
				}
        		stage = 1;
        		socket.emit("Java_ConnectHyracksRes", "ok");
        	}
        });
        socket.on("Java_Rules1Req", new Emitter.Listener() {
        	@Override
        	public void call(Object... args) {
        		rules1 = (JSONObject)args[0];
        		stage = 4;
        		socket.emit("Java_Rules1Res", "ok");
        	}
        });
        socket.on("Java_Rules2Req", new Emitter.Listener() {
        	@Override
        	public void call(Object... args) {
        		rules2 = (JSONObject)args[0];
        		if(stage == 4) {
        			try {
						proc(); // at this point, all data and rules are ingested, start cleaning tasks
					} catch (JSONException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
        		}
        		stage = 5;
        		socket.emit("Java_Rules2Res", "ok");
        	}
        });
        socket.on("Java_ConnectSQL1Req", new Emitter.Listener() {
        	@Override
        	public void call(Object... args) {
        		JSONObject sql1 = (JSONObject)args[0];
        		// import all data from 1st mysql db
                MySQLDB db = null;
				try {
					db = new MySQLDB(sql1.getString("sql1ip"), "3306", sql1.getString("sql1db"), sql1.getString("sql1user"), sql1.getString("sql1pw"));
				} catch (JSONException e) {
					e.printStackTrace();
				}
                ArrayList<String> tables = null;
				try {
					tables = db.getTables(sql1.getString("sql1db"));
				} catch (JSONException e) {
					e.printStackTrace();
				}
                Map<String, ArrayList<ArrayList<String>>> cols = new HashMap<String, ArrayList<ArrayList<String>>>();
                for(int i = 0; i < tables.size(); i++) {
                	ArrayList<ArrayList<String>> t = db.getColumns(tables.get(i));
                	cols.put(tables.get(i).toString(), t);
                }
                for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : cols.entrySet()) {
                	ArrayList<String> colnames = new ArrayList<String>();
                	for(int i = 0; i < entry.getValue().size(); i++) {
                		colnames.add(entry.getValue().get(i).get(0));
                	}
                	data1.put(entry.getKey(), db.get("SELECT * FROM `"+entry.getKey()+"`", colnames));
                }
                db.disconnect();
                stage = 2;
        		socket.emit("Java_ConnectSQL1Res", new JSONObject(cols));
        	}
        });
        socket.on("Java_ConnectSQL2Req", new Emitter.Listener() {
        	@Override
        	public void call(Object... args) {
        		JSONObject sql2 = (JSONObject)args[0];
        		// import all data from 2nd mysql db
                MySQLDB db2 = null;
				try {
					db2 = new MySQLDB(sql2.getString("sql2ip"), "3306", sql2.getString("sql2db"), sql2.getString("sql2user"), sql2.getString("sql2pw"));
				} catch (JSONException e) {
					e.printStackTrace();
				}
                ArrayList<String> tables2 = null;
				try {
					tables2 = db2.getTables(sql2.getString("sql2db"));
				} catch (JSONException e) {
					e.printStackTrace();
				}
                Map<String, ArrayList<ArrayList<String>>> cols2 = new HashMap<String, ArrayList<ArrayList<String>>>();
                for(int i = 0; i < tables2.size(); i++) {
                	ArrayList<ArrayList<String>> t = db2.getColumns(tables2.get(i));
                	cols2.put(tables2.get(i).toString(), t);
                }
                for(Map.Entry<String, ArrayList<ArrayList<String>>> entry : cols2.entrySet()) {
                	ArrayList<String> colnames = new ArrayList<String>();
                	for(int i = 0; i < entry.getValue().size(); i++) {
                		colnames.add(entry.getValue().get(i).get(0));
                	}
                	data2.put(entry.getKey(), db2.get("SELECT * FROM `"+entry.getKey()+"`", colnames));
                }
                db2.disconnect();
                stage = 3;
        		socket.emit("Java_ConnectSQL2Res", new JSONObject(cols2));
        	}
        });
    }
}
