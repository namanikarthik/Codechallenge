package com.ms3;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class Ms3Application {

	public static void main(String[] args) {
		Connection conn = null;
		int count = 0;
		try {
			Class.forName("org.sqlite.JDBC");
			String url = "jdbc:sqlite:C:/SQLite/ms3.db";
			conn = DriverManager.getConnection(url);
			System.out.println("Connection to SQLite has been established.");
			String createTable = "CREATE TABLE IF NOT EXISTS TableX (A text,B text,C text,D text,E text,F text,G text,H text,I text,J text)";
			Statement stmt = conn.createStatement();
			stmt.execute(createTable);
			String insertSql = "INSERT INTO TableX(A,B,C,D,E,F,G,H,I,J) VALUES(?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement pstmt = conn.prepareStatement(insertSql);
			List<String[]> readCSVfile = readCSVfile();
			
			for (String[] row : readCSVfile) {
				count++;
				int index = 0;
				for (String cell : row) {
					index++;
					pstmt.setString(index, cell);
					if(index==10)
						break;
				}
				pstmt.executeUpdate();

			}
		} catch (SQLException | ClassNotFoundException e) {
			System.err.println("error " + e.getMessage());
		} finally {
			System.out.println("No of Records : " +count+" Inserted sucessfully");
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException ex) {
				System.err.println(ex.getMessage());
			}
		}
	}

	private static List<String[]> readCSVfile() {
		String path = "E:/Projects/ms3Interview.csv";
		List<String[]> readAll = null;
		FileReader filereader;
		try {
			filereader = new FileReader(new File(path));
			CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(1).build();
			readAll = csvReader.readAll();
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
		return readAll;
	}
}
