package com.rt14.cop5725;

// IMPORTS
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

// Client class to interact with AsterixDB server via its HTTP API
public class ADBClient {
	private String url;
	private OkHttpClient client;
	
	ADBClient(String url) {
		this.url = url;
		this.client = new OkHttpClient();
	}
	
	// Execute HTTP POST request following the format specified in AsterixDB documentation
	// https://asterixdb.apache.org/docs/0.9.4.1/api.html
	public Response post(String path, String value) throws UnsupportedEncodingException {
		Response res = null;
		RequestBody rb = new FormBody.Builder()
        		.add("statement", value)
        		.add("pretty", "false")
        		.add("mode", "async")
        	.build();
        Request req = new Request.Builder().url(this.url + path).post(rb).build();
        try {
        	res = this.client.newCall(req).execute();
        	return res;
        }catch(IOException e) {
        	e.printStackTrace();
        }
        return res;
	}
}
