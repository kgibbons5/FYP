package test;
import java.sql.*;
/**
 *
 * @author 11478822
 */
public class Connect {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        
        Class.forName("com.mysql.jdbc.Driver");
        
        Connection con = DriverManager.getConnection("jdbc:mysql://danu6.it.nuigalway.ie:3306/mydb1803","mydb1803gk","ki1riw");
     
        
        Statement stmt = con.createStatement(); 
        String createLehigh = "Create table Lehigh " + "(SSN Integer not null, Name VARCHAR(32), " + "Marks Integer)";
        stmt.executeUpdate(createLehigh);

        String insertLehigh = "Insert into Lehigh " + "VALUES (123456789,'abc',100)";
        stmt.executeUpdate(insertLehigh); 
        
        String queryLehigh = "select * from Lehigh";
        ResultSet rs = stmt.executeQuery(queryLehigh);
        //What does this statement do?
        while (rs.next()) {
            int ssn = rs.getInt("SSN");
            String name = rs.getString("NAME");
            int marks = rs.getInt("MARKS");
            
            System.out.println("ssn:" +ssn);
            System.out.println("ssn:" +name);
            System.out.println("ssn:" +marks);
            
     }
           
        stmt.close();
        con.close(); 
       
    }
    
    
}
