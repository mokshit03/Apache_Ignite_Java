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
            igniteConnection = DriverManager.getConnection("jdbc:ignite:thin://USBLRMOKBHA1.us.deloitte.com:10800");

            //INSERT INTO CACHE
            PreparedStatement pstmt = igniteConnection.prepareStatement("INSERT INTO employee (id, name, country) VALUES (?, ?, ?)");
            pstmt.setString(1, "8");
            pstmt.setString(2, "Mehul 3");
            pstmt.setString(3, "India");
            pstmt.executeUpdate();
            pstmt.close();

            //DELETE INTO CACHE
            pstmt = igniteConnection.prepareStatement("DELETE FROM employee WHERE id = ?");
            pstmt.setString(1, "8");
            pstmt.executeUpdate();
            System.out.println("Deleted record with id 4");
            pstmt.close();

            //UPDATE INTO CACHE
            pstmt = igniteConnection.prepareStatement("UPDATE employee SET  country = 'India' WHERE id IN(1,2,3)");
            pstmt.executeUpdate();
            System.out.println("Updated records with id 1,2,3");
            pstmt.close();

            //SELECT INTO CACHE
            Statement stmt = igniteConnection.createStatement();
            rs = stmt.executeQuery("SELECT e.id, e.name, e.country FROM Employee e");

            while(rs.next()){
                String id = rs.getString("id");
                String name = rs.getString("name");
                String country = rs.getString("country");
                System.out.println(id+"\t"+name+"\t"+country);
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
