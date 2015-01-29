package test;
import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import javax.swing.JOptionPane;
/**
 *
 * @author 11478822
 */
public class Connect {

    static final int CATEGORY = 0;
    static final int SRC_TERM = 1;
    static final int SRC_LANG = 2;
    static final int SRC_CONTEXT = 3;
    static final int TARG_LANG = 4;
    static final int TARG_TERM = 5;
    static final int TARG_CONTEXT = 6;
    //do for rest
    // could use enum instead
    
    
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
      
        
        
        //TODO: Check if file exisits, if not exit
        
        //TODO: could reimplement for command line
        //TODO: reading about INI format
        String filename = "C://Users//Katie//Documents//College//FYP//data//glossary_italian_english_conflict_terrorism_big.csv";
        
        
        Class.forName("com.mysql.jdbc.Driver");
                
        Connection con = DriverManager.getConnection("jdbc:mysql://danu6.it.nuigalway.ie:3306/mydb1803","mydb1803gk","ki1riw");
        PreparedStatement pst = null;     
        Statement stmt=null;
        ResultSet rs=null;
        PreparedStatement pstmt=null;
        // Create hashtables for loading data from database to memeory
        Hashtable<String, Long> ht_languages = new Hashtable<String, Long>();
        
        long src_lang_id=0; // key                
        long targ_lang_id=0; // key  
        int src_term_id=0;
        int targ_term_id=0;
        
        
        try{
              stmt=con.createStatement();
              //loading exiting values from languages table to hashtable
              rs=stmt.executeQuery("SELECT * FROM languages;");
              while (rs.next()){
                  ht_languages.put(rs.getString(2), (long)rs.getInt(1));
              }

        }finally{
          rs.close();
          stmt.close();
        }
    
        
        try{
            
            BufferedReader br = new BufferedReader(new FileReader(filename));
            // TODO: What will happen if file doesnt exist
            String line;

            while((line=br.readLine())!=null)
            {
                
                //TODO: Add parsi                
                //String[]value =line.split(","); //seperator
                String[]values = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                for(int i=0; i<values.length; i++)
                {
                    // remove quotations
                    values[i] = values[i].replace("\"","");
                }
                               
                // processing languages, required variable, cannot get proper language, cannot add to table
                // stop processing this line, move to next and report an error
                values[SRC_LANG] = values[SRC_LANG].toLowerCase();
                // if missing
                if(ht_languages.get(values[SRC_LANG]) == null){
                    // insert language and get id of newly inserted language
                    pstmt = con.prepareStatement("Insert into languages (language) VALUES (?);"  ,
                                          Statement.RETURN_GENERATED_KEYS);
                    pstmt.setString(1, values[SRC_LANG]);
                 
                    if( pstmt.executeUpdate()!=0){
                        // if successful
                        
                        // get id
//                        rs = pstmt.getGeneratedKeys();
//                        while(rs.next()){
//                            src_lang_id = rs.getInt(1);
//                        }
                        
                        // return new id(primary key)
                        try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                            if (generatedKeys.next()) {
                                // assigning new value to new language in hashtable (cache)
                                // use only with known size, not unlimitted amount of element e.g. terms
                                ht_languages.put(values[SRC_LANG], generatedKeys.getLong(1));
                                src_lang_id = generatedKeys.getInt(1);
                            }
                                      //TODO: Report error
                        }                                
                    }
                }    
                              
                
                values[TARG_LANG] = values[TARG_LANG].toLowerCase();
                      
                if(ht_languages.get(values[TARG_LANG]) == null){
                    // insert language and get id of newly inserted language
                    pstmt = con.prepareStatement("Insert into languages (language) VALUES (?);",
                            Statement.RETURN_GENERATED_KEYS);
                    pstmt.setString(1, values[TARG_LANG]);
                    if( pstmt.executeUpdate()!=0){
                        // if successful
                        
                        // get id
//                        rs = pstmt.getGeneratedKeys();
//                        while(rs.next()){
//                            targ_lang_id = rs.getInt(1);
//                        }
                        
                        // return new id(primary key)
                        try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                            if (generatedKeys.next()) {
                                // assigning new value to new language in hashtable (cache)
                                // use only with known size, not unlimitted amount of element e.g. terms
                                ht_languages.put(values[TARG_LANG], generatedKeys.getLong(1));
                                targ_lang_id = (int) generatedKeys.getInt(1);
                            }
                                      //TODO: Report error
                        }                                
                    }
                                  
                }//close if
                
                
       
                //Insert into terms table
                try{
                    
                    // src_term
                    pstmt = con.prepareStatement("Insert into terms (language_id, term) VALUES (?,?);"  ,
                                          Statement.RETURN_GENERATED_KEYS);
                    pstmt.setLong(1, src_lang_id);
                    pstmt.setString(2, values[SRC_TERM]);
                    
                    pstmt.executeUpdate();
                    
                    rs= pstmt.getGeneratedKeys();
                        while(rs.next()){
                            src_term_id = rs.getInt(1);
                        }
                        
                    System.out.println("*****SOURCE TERM ID IS: " + src_term_id);
                    
                    
                    // targ_term
                    pstmt = con.prepareStatement("Insert into terms (language_id, term) VALUES (?,?);"  ,
                                          Statement.RETURN_GENERATED_KEYS);
                    pstmt.setLong(1, targ_lang_id);
                    pstmt.setString(2, values[TARG_TERM]);
                    
                    pstmt.executeUpdate();
                     rs= pstmt.getGeneratedKeys();
                        while(rs.next()){
                            targ_term_id = rs.getInt(1);
                        }
                        
                    System.out.println("*****TARGET TERM ID IS: " + targ_term_id);
                    
                }
                catch(Exception e){
                    JOptionPane.showMessageDialog(null, e);
                }

                  //Insert into terms translations
                try{ 
                    
                    pstmt = con.prepareStatement("Insert into translations (src_term_id, targ_term_id) VALUES (?,?);"  ,
                                          Statement.RETURN_GENERATED_KEYS);
                    pstmt.setLong(1, src_term_id);
                    pstmt.setLong(2, targ_term_id);
                    
                    pstmt.executeUpdate();
                    
//                    rs= pstmt.getGeneratedKeys();
//                        while(rs.next()){
//                            src_term_id = rs.getInt(1);
//                        }
                        
                  //  System.out.println("*****SOURCE TERM ID IS: " + src_term_id);
   
                    
                }
                catch(Exception e){
                    JOptionPane.showMessageDialog(null, e);
                }
                  
                  
                  
                
            }
            //do something with language
            for (String key : ht_languages.keySet()) {
                System.out.println("key: " + key + " value: " + ht_languages.get(key));
            }

     }
     
     catch(Exception e){
         JOptionPane.showMessageDialog(null, e);
     }
        
        finally
        {
            if(con != null)
            {
                con.close();
            }
            
            if(pst != null)
            {
                pst.close();
            }
        }
     
    }    
}
