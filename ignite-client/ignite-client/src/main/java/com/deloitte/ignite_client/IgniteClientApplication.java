package com.deloitte.ignite_client;

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
            igniteConnection = DriverManager.getConnection("jdbc:ignite:thin://USBLRMOKBHA1.us.deloitte.com/127.0.0.1:10800?SCHEMA=PUBLIC");

            PreparedStatement pstm = igniteConnection.prepareStatement("SELECT TABLE_NAME, CACHE_NAME FROM SYS.TABLES WHERE TABLE_NAME='EMPLOYEE'");
            rs=pstm.executeQuery();
            while(rs.next())
            {
                System.out.println(rs.getString(1) + rs.getString(2));
            }
        //     //1. NUMBER OF ROWS IN OUR CACHE -
            int n=10;
            long duration=0;
            PreparedStatement pstmt = igniteConnection.prepareStatement("SELECT count(*) from EMPLOYEE");
            while(n>0)
            {
                long start=System.nanoTime();
                rs=pstmt.executeQuery();
                if(rs.next())
                {
                    System.out.println("TOTAL RECORDS ARE  "+" " +rs.getString(1));
                }
                long end=System.nanoTime();
                duration=duration+(end-start);
                if(n==1)
                {
                    System.out.println(Thread.currentThread().getName());
                }
                n--;
            }
            System.out.println(Thread.currentThread().getName());
            long avg = duration/10;
            System.out.println("Time Take To get Count   -  "+ avg +"  Time" );

        
        //     //3. INSERT INTO CACHE
            duration=0;
            String[] myid={"100006", "100007", "100008", "100009", "100010, 100011, 100012, 100013, 100014, 100014, 100016, 100017, 100018, 100019, 100020"};
            int x=myid.length-1;
            int i=x;
            pstmt = igniteConnection.prepareStatement("INSERT INTO EMPLOYEE (id, name, country) VALUES (?, ?, ?)");
            while(i>0)
            {
                pstmt.setString(1, myid[i]);
                pstmt.setString(2, "MOTUUU");
                pstmt.setString(3, "USA");
                long start=System.nanoTime();
                pstmt.executeUpdate();
                long end=System.nanoTime();
                duration=duration+(end-start);
                i--;
            }
            avg=duration/x;
            System.out.println("INSETED RECORD with id :  1,00,003 in   - " + (avg) + " Time");
            System.out.flush();
            pstmt.close();
            
            //2. SELECT INTO CACHE -
            pstmt = igniteConnection.prepareStatement("SELECT id, name, country FROM employee");
            n=10;
             duration=0;
            while(n>0)
            {
            long startTime = System.nanoTime();
            ResultSet rs = pstmt.executeQuery();
            int count=0;
            
            while(rs.next())
            {
               rs.getString("id"); 
               rs.getString("name");
               rs.getString("country");
               count++; 
            }
            System.out.println(count);
            long endTime = System.nanoTime();
            duration=duration+(endTime-startTime);
            n--;
        }
         avg = duration/10;
                System.out.println("SELECTED ALL RECORDS FROM CACHE  IN:  - "+(avg) +" Time" );
                rs.close();
                //stmt.close();


        //     //4. UPDATE INTO CACHE
            n=10;
            duration=0;
            pstmt = igniteConnection.prepareStatement("UPDATE employee SET  country = 'India' WHERE id IN (100003)");
            while(n>0)
            {
            long start=System.nanoTime();
            pstmt.executeUpdate();
            long end=System.nanoTime();
            duration=duration+(end-start);
            n--;
            }
            avg=duration/10;
            System.out.println("UPDATED RECORD WITH ID:  1,00,003 in   -  " + (avg) + "Time");
            System.out.flush();
            pstmt.close();

        //     //5. CHECKING IF DATA IS INSERTED & UPDATED
            n=10;
            duration=0;
            while(n>0)
            {
            long start=System.nanoTime();
            pstmt = igniteConnection.prepareStatement("SELECT id, name, country FROM employee WHERE id='100005'");
            rs= pstmt.executeQuery();
            while(rs.next())
            {
                System.out.println(rs.getString("id") +"  "+ rs.getString("name") +"  "+rs.getString("country"));
            }
            long end=System.nanoTime();
            duration=duration+(end-start);
            n--;
        }
            avg=duration/10;
            System.out.println("ID: 1,00,005 EXISTS & IS UPDATED in   -  "+ (avg)+ " Time");
            pstmt.close();

            // //6. DELETE INTO CACHE
            pstmt = igniteConnection.prepareStatement("DELETE FROM employee");
            long start=System.nanoTime();
            pstmt.executeUpdate();
            long end=System.nanoTime();
            long dura= end-start;
            System.out.println("DELETED ALL RECORDS FROM THE CACHE in  "+ (dura)+"  Time");        
            pstmt.close();

            // //7. COUNT AFTER DELETION 
            pstmt = igniteConnection.prepareStatement("SELECT count(*) from employee");
            rs=pstmt.executeQuery();
            if(rs.next())
            {
                System.out.println("TOTAL RECORDS AFTER DELETION  "+" " +rs.getString(1));
            }
            igniteConnection.close();
            System.out.flush();
     }
      catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
    }
}
}
