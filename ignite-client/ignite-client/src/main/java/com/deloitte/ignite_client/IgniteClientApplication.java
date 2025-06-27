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

            //1. NUMBER OF ROWS IN OUR CACHE - 
            PreparedStatement pstmt = igniteConnection.prepareStatement("SELECT count(*) from employee");
            rs=pstmt.executeQuery();
            if(rs.next())
            {
                System.out.println("TOTAL RECORDS ARE  "+" " +rs.getString(1));
            }

            //2. SELECT INTO CACHE -
            Statement stmt = igniteConnection.createStatement();
            long startTime = System.nanoTime();
            rs = stmt.executeQuery("SELECT e.id, e.name, e.country FROM Employee e");
            int count=0;
            while(rs.next()){
                String id = rs.getString("id");
                String name = rs.getString("name");
                String country = rs.getString("country");
                count++;
                //System.out.println(id+"\t"+name+"\t"+country);
            }
            long endTime = System.nanoTime();
            //(endTime-startTime)
            System.out.println("SELECTED "+ count+ " RECORDS FROM CACHE"+(endTime-startTime) );
            rs.close();
            stmt.close();
            //igniteConnection.close();

            //3. INSERT INTO CACHE
            pstmt = igniteConnection.prepareStatement("INSERT INTO employee (id, name, country) VALUES (?, ?, ?)");
            pstmt.setString(1, "100004");
            pstmt.setString(2, "MOKSHIT BHANDARI");
            pstmt.setString(3, "USA");
            pstmt.executeUpdate();
            System.out.println("INSETED RECORD with id :  1,00,004");
            pstmt.close();
            
            //4. UPDATE INTO CACHE
            pstmt = igniteConnection.prepareStatement("UPDATE employee SET  country = 'India' WHERE id IN (100004)");
            pstmt.executeUpdate();
            System.out.println("UPDATED RECORDS with id :  1,00,004");
            pstmt.close();

            //5. CHECKING IF DATA IS INSERTED & UPDATED
            pstmt = igniteConnection.prepareStatement("SELECT id, name, country FROM employee WHERE id='100004'");
            rs= pstmt.executeQuery();
            while(rs.next())
            {
                System.out.println(rs.getString("id") +"  "+ rs.getString("name") +"  "+rs.getString("country"));
            }
            System.out.println("ID: 1,00,004 EXISTS & IS UPDATED");
            pstmt.close();

            //6. DELETE INTO CACHE
            pstmt = igniteConnection.prepareStatement("DELETE FROM employee");
            pstmt.executeUpdate();
            System.out.println("DELETED ALL RECORDS FROM THE CACHE");
            pstmt.close();

            //7. COUNT AFTER DELETION 
            pstmt = igniteConnection.prepareStatement("SELECT count(*) from employee");
            rs=pstmt.executeQuery();
            if(rs.next())
            {
                System.out.println("TOTAL RECORDS AFTER DELETION  "+" " +rs.getString(1));
            }
            igniteConnection.close();
     }
      catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
    }
}
}
