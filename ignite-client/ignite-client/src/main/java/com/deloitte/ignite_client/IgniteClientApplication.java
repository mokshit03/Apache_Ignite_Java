package com.deloitte.ignite_client;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class IgniteClientApplication {

	private static Connection igniteConnection;
    private static ResultSet rs;
    public static void main(String[] args) {
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

            // Use the correct JDBC URL format for Ignite
            igniteConnection = DriverManager.getConnection("jdbc:ignite:thin://USBLRMOKBHA1.us.deloitte.com:10800");

            //Use separate variables for Statement and PreparedStatement
            PreparedStatement pstmt = igniteConnection.prepareStatement("INSERT INTO employee (id, name, country) VALUES (?, ?, ?)");

            pstmt.setString(1, "4");
            pstmt.setString(2, "James");
            pstmt.setString(3, "EEUU");
            pstmt.executeUpdate();
            pstmt.close();

            Statement stmt = igniteConnection.createStatement();
            rs = stmt.executeQuery("SELECT e.name, e.country FROM Employee e");

            while(rs.next()){
                String name = rs.getString(1);
                String country = rs.getString(2);
                System.out.println(name+"\t"+country);
            }

            rs.close();
            stmt.close();
            igniteConnection.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
