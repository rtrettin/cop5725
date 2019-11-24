package com.rt14.cop5725;

// IMPORTS
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class MySQLDB {
	// MySQL driver class to import dirty data
	private static PreparedStatement prepstat = null;
	
	private String host;
	private String port;
	private String db;
	private String user;
	private String password;
	private Connection conn;
	
	// constructor, setup a connection to the MySQL server
	public MySQLDB(String host, String port, String db, String user, String password) {
		this.host = host;
		this.port = port;
		this.db = db;
		this.user = user;
		this.password = password;
		this.conn = MySQLDB.connect(this.host, this.port, this.db, this.user, this.password);
	}
	
	// private function to establish the connection
	private static Connection connect(String host, String port, String db, String user, String password) {
		Connection c = null;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
		}catch(ClassNotFoundException e) {
			System.out.println("could not find com.mysql.cj.jdbc.Driver class");
			e.printStackTrace();
			return c;
		}
		try {
			c = DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+db, user, password);
			if(c == null) {
				System.out.println("MySQL connection failed");
				return c;
			}
			return c;
		}catch(SQLException e) {
			System.out.println("MySQL connection failed");
			e.printStackTrace();
			return c;
		}
	}
	
	// retrieve all data from a table (SELECT * FROM) given all column names in the table
	public ArrayList<ArrayList<String>> get(String sql, ArrayList<String> colnames) {
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		try {
			prepstat = this.conn.prepareStatement(sql);
			ResultSet rs = prepstat.executeQuery();
			while(rs.next()) {
				ArrayList<String> t = new ArrayList<String>();
				for(int i = 0; i < colnames.size(); i++) {
					t.add(rs.getString(colnames.get(i).toString()));
				}
				result.add(t);
			}
			rs.close();
			return result;
		}catch(SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	// retrieve all column names in the given table
	public ArrayList<ArrayList<String>> getColumns(String tbl) {
		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		try {
			prepstat = this.conn.prepareStatement("SELECT `COLUMN_NAME`,`COLUMN_TYPE`,`IS_NULLABLE` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA` = '"+this.db+"' AND `TABLE_NAME` = '"+tbl+"'");
			ResultSet rs = prepstat.executeQuery();
			while(rs.next()) {
				String cn = rs.getString("COLUMN_NAME");
				String ct = rs.getString("COLUMN_TYPE");
				String in = rs.getString("IS_NULLABLE");
				ArrayList<String> t = new ArrayList<String>();
				t.add(cn);
				t.add(ct);
				t.add(in);
				list.add(t);
			}
			rs.close();
			return list;
		}catch(SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	// retrieve all table names within the given database
	public ArrayList<String> getTables(String db) {
		ArrayList<String> result = new ArrayList<String>();
		try {
			prepstat = this.conn.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+db+"'");
			ResultSet rs = prepstat.executeQuery();
			while(rs.next()) {
				result.add(rs.getString("table_name"));
			}
			rs.close();
			return result;
		}catch(SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	// close the MySQL connection cleanly
	public void disconnect() {
		try {
			this.conn.close();
		}catch(SQLException e) {
			e.printStackTrace();
		}
	}
}
