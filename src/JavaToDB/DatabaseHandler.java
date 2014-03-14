package JavaToDB;

import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseHandler {
  private Connection connect = null;
  private Statement statement = null;
  private PreparedStatement preparedStatement = null;
  private ResultSet resultSet = null;
  
  public static void main(String[] args) throws Exception {
    DatabaseHandler dao = new DatabaseHandler();
    dao.readDataBase();
  }

  public void readDataBase() throws Exception {
    try {
    	openDBconn();

    	//insertDB();
    	//insertDBActiveFriends("2324232", "23263423777");
    	insertDBgroups("2324232", "23263423777");
     // writeMetaData(resultSet);
      
    } catch (Exception e) {
      throw e;
    } finally {
      close();
    }

  }

  public void writeMetaData(ResultSet resultSet) throws SQLException {
    //   Now get some metadata from the database
    // Result set get the result of the SQL query
    
    System.out.println("The columns in the table are: ");
    
    System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
    for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
      System.out.println("Column " +i  + " "+ resultSet.getMetaData().getColumnName(i));
    }
  }

  public void writeResultSet(ResultSet resultSet) throws SQLException {
    // ResultSet is initially before the first data set
    while (resultSet.next()) {
      // It is possible to get the columns via name
      // also possible to get the columns via the column number
      // which starts at 1
      // e.g. resultSet.getSTring(2);
      String userid = resultSet.getString("userid");
      String gender = resultSet.getString("gender");
      String languages = resultSet.getString("languages");
      String current_loc = resultSet.getString("current_locationY");
      String hometown_loc = resultSet.getString("hometown_location");
      String religion = resultSet.getString("religion");
      String education = resultSet.getString("education");
      String work_history = resultSet.getString("work_history");
      String significant_other_id = resultSet.getString("significant_other_id");
      String relationship_status = resultSet.getString("relationship_status");
      System.out.println("userid: " + userid);
      System.out.println("gender: " + gender);
      System.out.println("languages: " + languages);
      System.out.println("current_loc: " + current_loc);
      System.out.println("hometown_loc: " + hometown_loc);
      System.out.println("religion: " + religion);
      System.out.println("education: " + education);
      System.out.println("work_history:" + work_history);
      System.out.println("significant_other_id:" + significant_other_id);
      System.out.println("relationship_status: " + relationship_status);
    }
  }
  
  public void openDBconn() throws Exception{
	  try {
	      // This will load the MySQL driver, each DB has its own driver
	      Class.forName("com.mysql.jdbc.Driver");
	      // Setup the connection with the DB
	      connect = DriverManager
	          .getConnection("jdbc:mysql://localhost/sp?"
	              + "user=root&password=Mae@0693");	      
	    } catch (Exception e) {
	      throw e;
	    } 
  }
  
  public void insertDB()throws Exception{
	  try {
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      // Result set get the result of the SQL query
	      resultSet = statement
	          .executeQuery("select * from SP.USER");
	      writeResultSet(resultSet);

	      // PreparedStatements can use variables and are more efficient
	      preparedStatement = connect
	          .prepareStatement("insert into  SP.USER values (default, ?, ?, ?, ? , ?, ?,?,?,?,?)");
	      // "myuser, webpage, datum, summary, COMMENTS from FEEDBACK.COMMENTS");
	      // Parameters start with 1
	      preparedStatement.setString(1, "120482368437394");
	      preparedStatement.setString(2, "Female");
	      preparedStatement.setString(3, "English;Tagalog");
	      preparedStatement.setString(4, "Home sweet Home");
	      preparedStatement.setString(5, "Cebu");
	      preparedStatement.setString(6, "Roman Catholic");
	      preparedStatement.setString(7, "UP Cebu; UP high");
	      preparedStatement.setString(8, "Work1; Work2");
	      preparedStatement.setString(9, "2323534354");
	      preparedStatement.setString(10, "Single");
	      preparedStatement.executeUpdate();
  
	    } catch (Exception e) {
	      throw e;
	    }  
  }
  
  public void insertDBActiveFriends(String uid, String theFriend)throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.FRIENDSHIPS where uid=? and friendID=?");
	      preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, theFriend);
	      
	      
	      rs = preparedStatement.executeQuery();
	      
	      /*while(rs.next()){
	    	  String s = rs.getString(2);
	    	  String r = rs.getString(3);
	    	  System.out.println(s + "jhfsd"+r);
	      }*/
	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
		      preparedStatement = connect
		          .prepareStatement("insert into  SP.FRIENDSHIPS values (default, ?, ?)");
		      // "myuser, webpage, datum, summary, COMMENTS from FEEDBACK.COMMENTS");
		      // Parameters start with 1
		      preparedStatement.setString(1, uid);
		      preparedStatement.setString(2, theFriend);
		      preparedStatement.executeUpdate();
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }
  
  public void insertDBgroups(String uid, String group)throws Exception{
	  try {
		  ResultSet rs;
		  System.out.println(group);
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.GROUPS where gid=?");
	      preparedStatement.setString(1, group);	      
	      
	      rs = preparedStatement.executeQuery();
	      System.out.println(preparedStatement);
	      

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
		      preparedStatement = connect
		          .prepareStatement("insert into  SP.GROUPS values (default, ?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, group);
		      preparedStatement.executeUpdate();
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }

  public void insertDBgroupmembership(String uid, String group)throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.GROUP_MEMBERSHIP where gid=? and uid=?");
	      preparedStatement.setString(1, group);	 
	      preparedStatement.setString(2, uid);
	      
	      rs = preparedStatement.executeQuery();

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
		      preparedStatement = connect
		          .prepareStatement("insert into  SP.GROUP_MEMBERSHIP values (default, ?,?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, group);
		      preparedStatement.setString(2, uid);
		      preparedStatement.executeUpdate();
	      }
	      
	    } catch (Exception e) {
	      throw e;
	    }  
  }
  // You need to close the resultSet
  
  public void insertDBlikePages(String uid, String pageID)throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.PAGE_LIKE where uid=? and pageID=?");
	      preparedStatement.setString(1, uid);	 
	      preparedStatement.setString(2, pageID);
	      
	      rs = preparedStatement.executeQuery();

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
		      preparedStatement = connect
		          .prepareStatement("insert into  SP.PAGE_LIKE values (default, ?,?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, uid);
		      preparedStatement.setString(2, pageID);
		      preparedStatement.executeUpdate();
	      }
	      
	    } catch (Exception e) {
	      throw e;
	    }
  }
  
  public Double getLikedPageSimilarityFactor(String uid, String friendID)throws Exception{
	  Double intersection=0.0, union=0.0;
	  try {
		  ResultSet rs, rs1, rs2;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.PAGE_LIKE where uid=?");
	      preparedStatement.setString(1, uid);
	      
	      rs = preparedStatement.executeQuery();
	      
	      while(rs.next()){
	    	  /*System.out.println(rs.getString(2));
	    	  System.out.println(rs.getString(3));*/
	    	  preparedStatement = connect
			          .prepareStatement("select * from  SP.PAGE_LIKE where uid=? and pageID=?");
	    	  preparedStatement.setString(1, friendID);
		      preparedStatement.setString(2, rs.getString(3));
		      
		      rs1 = preparedStatement.executeQuery();
		      
		      if(rs1.next()){
		    	  intersection++;
		      }
	      }
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select * from  SP.PAGE_LIKE where uid=? or uid=?) as x");
    	  preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, friendID);
	      
	      rs2 = preparedStatement.executeQuery();
	      if(rs2.next()){
	    	  union = Double.parseDouble(rs2.getString(1));
	      }
     
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(union==0.0)
		  return 0.0;
	  else
		  return intersection/union;
  }

  public Double getFriendsSimilarityFactor(String uid, String friendID)throws Exception{
	  Double intersection=0.0, union=0.0;
	  try {
		  ResultSet rs, rs1, rs2;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.FRIENDSHIPS where uid=?");
	      preparedStatement.setString(1, uid);
	      
	      rs = preparedStatement.executeQuery();
	      
	      while(rs.next()){
	    	  /*System.out.println(rs.getString(2));
	    	  System.out.println(rs.getString(3));*/
	    	  preparedStatement = connect
			          .prepareStatement("select * from  SP.FRIENDSHIPS where uid=? and friendID=?");
	    	  preparedStatement.setString(1, friendID);
		      preparedStatement.setString(2, rs.getString(3));
		      
		      rs1 = preparedStatement.executeQuery();
		      
		      if(rs1.next()){
		    	  intersection++;
		      }      
	      }
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select * from  SP.FRIENDSHIPS where uid=? or uid=?) as x");
    	  preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, friendID);
	      
	      rs2 = preparedStatement.executeQuery();
	      if(rs2.next()){
	    	  union = Double.parseDouble(rs2.getString(1));
	      }
     
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(union==0.0)
		  return 0.0;
	  else
		  return intersection/union;
  }
  
  public Double getGroupsSimilarityFactor(String uid, String friendID)throws Exception{
	  Double intersection=0.0, union=0.0;
	  try {
		  ResultSet rs, rs1, rs2;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.GROUP_MEMBERSHIP where uid=?");
	      preparedStatement.setString(1, uid);
	      
	      rs = preparedStatement.executeQuery();
	      
	      while(rs.next()){
	    	  /*System.out.println(rs.getString(2));
	    	  System.out.println(rs.getString(3));*/
	    	  preparedStatement = connect
			          .prepareStatement("select * from  SP.GROUP_MEMBERSHIP where gid=? and uid=?");
	    	  preparedStatement.setString(1, rs.getString(2));
		      preparedStatement.setString(2, friendID);
		      
		      rs1 = preparedStatement.executeQuery();
		      
		      if(rs1.next()){
		    	  intersection++;
		      }      
	      }
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select * from  SP.FRIENDSHIPS where uid=? or uid=?) as x");
    	  preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, friendID);
	      
	      rs2 = preparedStatement.executeQuery();
	      if(rs2.next()){
	    	  union = Double.parseDouble(rs2.getString(1));
	      }
     
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(union==0.0)
		  return 0.0;
	  else
		  return intersection/union;
  }

  public Double getbasicInfoSimilarityFactor(String uid, String friendID)throws Exception{
	  Double intersection=0.0, union=0.0;
	  try {
		  ResultSet rs, rs1;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.USER where uid=?"); //sender's profile
	      preparedStatement.setString(1, uid);	      
	      rs = preparedStatement.executeQuery();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.USER where uid=?"); //receiver's profile
              preparedStatement.setString(1, friendID);	      
	      rs1 = preparedStatement.executeQuery();
	      
	      if(rs.next() && rs1.next()){
	    	  //get gender
	    	  String gender = rs.getString(3);
	    	  String gender1 = rs1.getString(3);
	    	  if(gender.equals(gender1)){
	    		  intersection++;
	    	  }	    	 
	    	  //get languages
	    	  String lang[] = rs.getString(4).split(";");
	    	  String lang1[] = rs1.getString(4).split(";");
	    	  for(int i=0;i<lang.length;i++){
	    		  if(isContained(lang1, lang[i])){
	    			  intersection++;
	    		  }
	    	  }
	    	//get current_location
	    	  String curr_loc = rs.getString(5);
	    	  String curr_loc1 = rs1.getString(5);
	    	  if(curr_loc.equals(curr_loc1)){
	    		  intersection++;
	    	  }
	    	 //get home_location
	    	  String home_loc = rs.getString(6);
	    	  String home_loc1 = rs1.getString(6);
	    	  if(home_loc.equals(home_loc1)){
	    		  intersection++;
	    	  }
	    	 //get religion
	    	  String religion = rs.getString(7);
	    	  String religion1 = rs1.getString(7);
	    	  if(religion.equals(religion1)){
	    		  intersection++;
	    	  }
	    	  //get school
	    	  String school[] = rs.getString(8).split(";");
	    	  String school1[] = rs1.getString(8).split(";");
	    	  for(int j=0;j<school.length;j++){
	    		  if(isContained(school1, school[j])){
	    			  intersection++;
	    		  }
	    	  }
	    	  //get work_history
	    	  String work[] = rs.getString(9).split(";");
	    	  String work1[] = rs1.getString(9).split(";");
	    	  for(int k=0;k<work.length;k++){
	    		  if(isContained(work1, work[k])){
	    			  intersection++;
	    		  }
	    	  }
	    	 //get relationship_status
	    	  String rel = rs.getString(11);
	    	  String rel1 = rs1.getString(11);
	    	  if(rel.equals(rel1)){
	    		  intersection++;
	    	  }
	    	  
	    	  union = (double)(lang.length + lang1.length + school.length + school1.length + work.length + work1.length + 10); //10 = count of 1directional info like(gender, religion, etc..) * 2 (users)
	      }    
     
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(union==0.0)
		  return 0.0;
	  else
		  return intersection/union;
  }
  
  public boolean isContained(String[] arr, String toFind){
	  for(int i=0;i<arr.length;i++){
		  if(arr[i].equals(toFind)){
			  return true;
		  }
	  }
	  
	  return false;
  }
  
  public Double getStatusLikesInteractivity(String uid, String friendID, String timeLimit) throws Exception{
	  Double aTob=0.0, bToa=0.0;
	  try {
		  ResultSet rs, rs1;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select status.uid as userID, status.postID, "
		          		+ "status_comments_likes.uid, status_comments_likes.statID from status_comments_likes join status "
		          		+ "on status_comments_likes.statID=status.postID where status_comments_likes.uid = ? and status.uid = ? "
		          		+ "and status_comments_likes.created_time IS NULL) as x"); //receiver's interaction towards sender
	      preparedStatement.setString(1, friendID);
	      preparedStatement.setString(2, uid);
	      preparedStatement.setString(3, timeLimit);
	      rs = preparedStatement.executeQuery();
	      rs.next();
	      bToa = Double.parseDouble(rs.getString(1));
	      
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select status.uid as userID, status.postID, "
		          		+ "status_comments_likes.uid, status_comments_likes.statID from status_comments_likes join status "
		          		+ "on status_comments_likes.statID=status.postID where status_comments_likes.uid = ? and status.uid = ? "
		          		+ "and status_comments_likes.created_time IS NULL) as x"); //sender's interaction towards receiver
	      preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, friendID);	 
	      preparedStatement.setString(3, timeLimit);
	      rs1 = preparedStatement.executeQuery();
	      rs1.next();
	      aTob = Double.parseDouble(rs1.getString(1));
        
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(aTob==0.0)
		  return 0.0;
	  else
		  return bToa/aTob;
  }

  public Double getStatusCommentsInteractivity(String uid, String friendID, String timeLimit) throws Exception{
	  Double aTob=0.0, bToa=0.0;
	  try {
		  ResultSet rs, rs1;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select status.uid as userID, status.postID, "
		          		+ "status_comments_likes.uid, status_comments_likes.statID from status_comments_likes join status "
		          		+ "on status_comments_likes.statID=status.postID where status_comments_likes.uid = ? and status.uid = ? and status_comments_likes.created_time < ? and status_comments_likes.created_time IS NOT NULL) as x"); //receiver's interaction towards sender
	      preparedStatement.setString(1, friendID);
	      preparedStatement.setString(2, uid);
	      preparedStatement.setString(3, timeLimit);
	      rs = preparedStatement.executeQuery();
	      rs.next();
	      bToa = Double.parseDouble(rs.getString(1));
	      
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select status.uid as userID, status.postID, "
		          		+ "status_comments_likes.uid, status_comments_likes.statID from status_comments_likes join status "
		          		+ "on status_comments_likes.statID=status.postID where status_comments_likes.uid = ? and status.uid = ? and status_comments_likes.created_time < ? and status_comments_likes.created_time IS NOT NULL) as x");//sender's interaction towards receiver
	      preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, friendID); 
	      preparedStatement.setString(3, timeLimit);
	      rs1 = preparedStatement.executeQuery();
	      rs1.next();
	      aTob = Double.parseDouble(rs1.getString(1));
        
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(aTob==0.0)
		  return 0.0;
	  else
		  return bToa/aTob;
  }
  
  public Double getPostsLikesInteractivity(String uid, String friendID, String timeLimit) throws Exception{
	  Double aTob=0.0, bToa=0.0;
	  try {
		  ResultSet rs, rs1;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select posts.uid as userID, posts.postID, "
		          		+ "posts_comments_likes.uid, posts_comments_likes.postID from posts_comments_likes join posts "
		          		+ "on posts_comments_likes.postID=posts.postID where posts_comments_likes.uid = ? and posts.uid = ? "
		          		+ "and posts_comments_likes.created_time IS NULL) as x"); //receiver's interaction towards sender
	      preparedStatement.setString(1, friendID);
	      preparedStatement.setString(2, uid);
	      preparedStatement.setString(3, timeLimit);
	      rs = preparedStatement.executeQuery();
	      rs.next();
	      bToa = Double.parseDouble(rs.getString(1));
	      
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select posts.uid as userID, posts.postID, "
		          		+ "posts_comments_likes.uid, posts_comments_likes.postID from posts_comments_likes join posts "
		          		+ "on posts_comments_likes.postID=posts.postID where posts_comments_likes.uid = ? and posts.uid = ? "
		          		+ "and posts_comments_likes.created_time IS NULL) as x"); //sender's interaction towards receiver
	      preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, friendID);	 
	      preparedStatement.setString(3, timeLimit);
	      rs1 = preparedStatement.executeQuery();
	      rs1.next();
	      aTob = Double.parseDouble(rs1.getString(1));
        
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(aTob==0.0)
		  return 0.0;
	  else
		  return bToa/aTob;
  }
  
  public Double getPostsCommentsInteractivity(String uid, String friendID, String timeLimit) throws Exception{
	  Double aTob=0.0, bToa=0.0;
	  try {
		  ResultSet rs, rs1;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select posts.uid as userID, posts.postID, "
		          		+ "posts_comments_likes.uid, posts_comments_likes.postID from posts_comments_likes join posts "
		          		+ "on posts_comments_likes.postID=posts.postID where posts_comments_likes.uid = ? and posts.uid = ? "
		          		+ "and posts_comments_likes.created_time < ? and posts_comments_likes.created_time IS NOT NULL) as x"); //receiver's interaction towards sender
	      preparedStatement.setString(1, friendID);
	      preparedStatement.setString(2, uid);
	      preparedStatement.setString(3, timeLimit);
	      rs = preparedStatement.executeQuery();
	      rs.next();
	      bToa = Double.parseDouble(rs.getString(1));
	      
	      
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select posts.uid as userID, posts.postID, "
		          		+ "posts_comments_likes.uid, posts_comments_likes.postID from posts_comments_likes join posts "
		          		+ "on posts_comments_likes.postID=posts.postID where posts_comments_likes.uid = ? and posts.uid = ? "
		          		+ "and posts_comments_likes.created_time < ? and posts_comments_likes.created_time IS NOT NULL) as x");//sender's interaction towards receiver
	      preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, friendID); 
	      preparedStatement.setString(3, timeLimit);
	      rs1 = preparedStatement.executeQuery();
	      rs1.next();
	      aTob = Double.parseDouble(rs1.getString(1));
        
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(aTob==0.0)
		  return 0.0;
	  else
		  return bToa/aTob;
  }
  
  public Double getConnectivity(String uid, String friendID, String timeLimit) throws Exception{
	  Double aToAll=0.0, bToa=0.0;
	  try {
		  ResultSet rs, rs1, rs2, rs3;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      //get receiver's count of interaction to sender
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select * from status_comments_likes where uid = ? and friendID=? and (status_comments_likes.created_time < ? or status_comments_likes.created_time IS NULL)) as x"); //receiver's count of interaction towards sender
	      preparedStatement.setString(1, friendID);
	      preparedStatement.setString(2, uid);
	      preparedStatement.setString(3, timeLimit);
	      rs = preparedStatement.executeQuery();
	      rs.next();
	      bToa = Double.parseDouble(rs.getString(1));
	      
	      //from posts
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select * from posts_comments_likes where uid = ? and friendID=? and (posts_comments_likes.created_time < ? or posts_comments_likes.created_time IS NULL)) as x"); //receiver's count of interaction towards sender
	      preparedStatement.setString(1, friendID);
	      preparedStatement.setString(2, uid);
	      preparedStatement.setString(3, timeLimit);
	      rs1 = preparedStatement.executeQuery();
	      rs1.next();
	      bToa += Double.parseDouble(rs1.getString(1));

	      
	      //get sender's interactions to all
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select * from status_comments_likes where uid = ? and (status_comments_likes.created_time < ? or status_comments_likes.created_time IS NULL)) as x"); //receiver's count of interaction towards sender
	      preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, timeLimit);
	      rs2 = preparedStatement.executeQuery();
	      rs2.next();
	      aToAll = Double.parseDouble(rs2.getString(1));
	      
	      //from posts
	      preparedStatement = connect
		          .prepareStatement("select count(*) from (select * from status_comments_likes where uid = ? and (status_comments_likes.created_time < ? or status_comments_likes.created_time IS NULL)) as x"); //receiver's count of interaction towards sender
	      preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, timeLimit);
	      rs3 = preparedStatement.executeQuery();
	      rs3.next();
	      aToAll += Double.parseDouble(rs3.getString(1));
	      
        
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  if(aToAll==0.0)
		  return 0.0;
	  else
		  return bToa/aToAll;
  }
  
  public ArrayList<String> getPostsfromOCTtoDEC(String uid)throws Exception{
	  ArrayList<String> posts = new ArrayList<String>();
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.status where uid=? and created_time >= '2013-10-01 00:00:00'");
	      preparedStatement.setString(1, uid);	 
	      
	      rs = preparedStatement.executeQuery();
	      
	      while(rs.next()){	    	 
		     posts.add(rs.getString(3)+";"+rs.getString(5) +";"+rs.getString(6));//format: postID;emotype;created_time
	      }      
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  return posts;
  }

  public ArrayList<String> getReceivers(String postID)throws Exception{
	  ArrayList<String> receivers = new ArrayList<String>();
	  try {
		  ResultSet rs, rs1;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.status_comments where statID=?");
	      preparedStatement.setString(1, postID);	 
	      
	      rs = preparedStatement.executeQuery();

	      while(rs.next()){	    	 
	    	  receivers.add(rs.getString(3));//format: user id of commentor
	      }	
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.status_likes where statID=?");
	      preparedStatement.setString(1, postID);	 
	      
	      rs1 = preparedStatement.executeQuery();

	      while(rs1.next()){
	    	  if(!receivers.contains(rs1.getString(2))){
	    		  receivers.add(rs1.getString(2));//format: user id of liker
	    	  }
	    	  
	      }	      
	    } catch (Exception e) {
	      throw e;
	    }
	  
	  return receivers;
  }
  
  public Double checkIfProbSaved(String uid, String friendID, String time)throws Exception{
	  Double prob;
	  
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.SIC_PROBABILITIES where senderID=? and receiverID=? and atThisTime=?");
	      preparedStatement.setString(1, uid);
	      preparedStatement.setString(2, friendID);
	      preparedStatement.setString(3, time);
	      
	      rs = preparedStatement.executeQuery();
	      
	      if(rs.next()){
	    	  prob = Double.parseDouble(rs.getString(4));
	    	  return prob;
	      }         
	  } catch (Exception e) {
	      throw e;
	  } 	  
	  return null;
  }
  
  public void insertSICprob(String uid, String friendID, Double prob, String time)throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.SIC_PROBABILITIES where senderID=? and receiverID=? and atThisTime=?");
	      preparedStatement.setString(1, uid);	 
	      preparedStatement.setString(2, friendID);
	      preparedStatement.setString(3, time);
	      
	      rs = preparedStatement.executeQuery();

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
		      preparedStatement = connect
		          .prepareStatement("insert into SP.SIC_PROBABILITIES values (default, ?,?,?,?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, uid);
		      preparedStatement.setString(2, friendID);
		      preparedStatement.setString(3, prob.toString());
		      preparedStatement.setString(4, time);
		      preparedStatement.executeUpdate();
	      } 
	  } catch (Exception e) {
	      throw e;
	  } 
  }
  
  public void insertactualandpredictedSICprob(String uid, String postID, Double actualprob, Double predicted, Double efficiency)throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.ACTUAL_VS_SICPREDICTION where uid=? and postID=?");
	      preparedStatement.setString(1, uid);	 
	      preparedStatement.setString(2, postID);
	      
	      rs = preparedStatement.executeQuery();

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
		      preparedStatement = connect
		          .prepareStatement("insert into  SP.SIC_PROBABILITIES values (default, ?,?,?,?,?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, uid);
		      preparedStatement.setString(2, postID);
		      preparedStatement.setString(3, actualprob.toString());
		      preparedStatement.setString(4, predicted.toString());
		      preparedStatement.setString(5, efficiency.toString());
		      preparedStatement.executeUpdate();
	      } 
	  } catch (Exception e) {
	      throw e;
	  } 
  }
  
  public String getStatReceiverEmotype(String receiverID, String postID, String created_time)throws Exception{
	  String emotype = "";
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from SP.STATUS where uid=? and postID=? and created_time > ?");
	      preparedStatement.setString(1, receiverID);	 
	      preparedStatement.setString(2, postID);
	      preparedStatement.setString(3, created_time);
	      
	      rs = preparedStatement.executeQuery();
	      
	      if(rs.next()){
		    emotype = rs.getString(5).trim();//5th column of table is for emotype
	      } 
	  } catch (Exception e) {
	      throw e;
	  }
	  
	  return emotype;
  }
  
  public void insertDBStatus(String postID, String message, String uid, String created_time, String emotype) throws Exception{
	  try {
                ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.STATUS where postID=? and message=? and uid=? and created_time=? and emotype=?");
	      preparedStatement.setString(1, postID);	      
	      preparedStatement.setString(2, message);	      
	      preparedStatement.setString(3, uid);	      
	      preparedStatement.setString(4, created_time);	      
	      preparedStatement.setString(5, emotype);
              
	      rs = preparedStatement.executeQuery();
	      System.out.println(preparedStatement);

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
	    	  
	    	  preparedStatement = connect
		          .prepareStatement("insert into  SP.STATUS (id, postID, message, uid, created_time, emotype) values (default, ?, ?, ?, ?, ?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, postID);
		      preparedStatement.setString(2, message);
		      preparedStatement.setString(3, uid);
		      preparedStatement.setString(4, created_time);
		      preparedStatement.setString(5, emotype);
		      preparedStatement.executeUpdate();
		      System.out.println("hererrrrrrrrrrrrrrrrreeeeeeeeeeeeeee");
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }
  
  public void insertDBStatusComment(String postID, String uid, String created_time) throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.STATUS_COMMENTS_LIKES where statID=? and uid=? and created_time=?");
	      preparedStatement.setString(1, postID);	      
	      preparedStatement.setString(2, uid);	      
	      preparedStatement.setString(3, created_time);	      
	      
	      rs = preparedStatement.executeQuery();
	      System.out.println(preparedStatement);

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
	    	  
	    	  preparedStatement = connect
		          .prepareStatement("insert into  SP.STATUS_COMMENTS_LIKES (id, statID, uid, created_time) values (default,  ?, ?, ?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, postID);
		      preparedStatement.setString(2, uid);
		      preparedStatement.setString(3, created_time);
		      preparedStatement.executeUpdate();
		      System.out.println("hererrrrrrrrrrrrrrrrreeeeeeeeeeeeeee");
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }

  public void insertDBStatusLike(String postID, String uid) throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.STATUS_COMMENTS_LIKES where statID=? and uid=?");
	      preparedStatement.setString(1, postID);	      
	      preparedStatement.setString(2, uid);	      
	      
	      rs = preparedStatement.executeQuery();
	      System.out.println(preparedStatement);

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
	    	  
	    	  preparedStatement = connect
		          .prepareStatement("insert into  SP.STATUS_COMMENTS_LIKES (id, statID, uid) values (default,  ?, ?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, postID);
		      preparedStatement.setString(2, uid);
		      preparedStatement.executeUpdate();
		      System.out.println("hererrrrrrrrrrrrrrrrreeeeeeeeeeeeeee");
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }

  public void insertDBPostComment(String postID, String uid, String created_time) throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.POSTS_COMMENTS_LIKES where postID=? and uid=? and created_time=?");
	      preparedStatement.setString(1, postID);	      
	      preparedStatement.setString(2, uid);	      
	      preparedStatement.setString(3, created_time);	      
	      
	      rs = preparedStatement.executeQuery();
	      System.out.println(preparedStatement);

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
	    	  
	    	  preparedStatement = connect
		          .prepareStatement("insert into  SP.POSTS_COMMENTS_LIKES (id, postID, uid, created_time) values (default,  ?, ?, ?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, postID);
		      preparedStatement.setString(2, uid);
		      preparedStatement.setString(3, created_time);
		      preparedStatement.executeUpdate();
		      System.out.println("hererrrrrrrrrrrrrrrrreeeeeeeeeeeeeee");
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }
  
  public void insertDBPost(String postID, String uid, String created_time) throws Exception {
       try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.POSTS where postID=? and uid=? and created_time=?");
	      preparedStatement.setString(1, postID);	      
	      preparedStatement.setString(2, uid);	      
	      preparedStatement.setString(3, created_time);	      
	      
	      rs = preparedStatement.executeQuery();
	      System.out.println(preparedStatement);

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
	    	  
	    	  preparedStatement = connect
		          .prepareStatement("insert into  SP.POSTS (id, postID, uid, created_time) values (default, ?, ?, ?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, postID);
		      preparedStatement.setString(2, uid);
		      preparedStatement.setString(3, created_time);
		      preparedStatement.executeUpdate();
		      System.out.println("hererrrrrrrrrrrrrrrrreeeeeeeeeeeeeee");
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }
  
  public void insertDBPostLikes(String postID, String uid) throws Exception {
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.POSTS_COMMENTS_LIKES where postID=? and uid=?");
	      preparedStatement.setString(1, postID);	      
	      preparedStatement.setString(2, uid);	      
	      
	      rs = preparedStatement.executeQuery();
	      System.out.println(preparedStatement);

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
	    	  
	    	  preparedStatement = connect
		          .prepareStatement("insert into  SP.POSTS_COMMENTS_LIKES (id, postID, uid) values (default, ?, ?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, postID);
		      preparedStatement.setString(2, uid);
		      preparedStatement.executeUpdate();
		      System.out.println("hererrrrrrrrrrrrrrrrreeeeeeeeeeeeeee");
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }
  
  public void insertDBBasicInfo(String userid, String gender, String languages, 
		  						String current_location, String hometown_location, 
		  						String religion, String education, String work_history, 
		  						String significant_other, String relationship_status) throws Exception{
	  try {
		  ResultSet rs;
		// Statements allow to issue SQL queries to the database
	      statement = connect.createStatement();
	      
	      preparedStatement = connect
		          .prepareStatement("select * from  SP.USER where userid=?");
	      preparedStatement.setString(1, userid);	      
	      
	      rs = preparedStatement.executeQuery();
	      System.out.println(preparedStatement);

	      if(!rs.next()){
		      // PreparedStatements can use variables and are more efficient
	    	  
	    	  preparedStatement = connect
		          .prepareStatement("insert into  SP.USER values (default, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		      // Parameters start with 1
		      preparedStatement.setString(1, userid);
		      preparedStatement.setString(2, gender);
		      preparedStatement.setString(3, languages);
		      preparedStatement.setString(4, current_location);
		      preparedStatement.setString(5, hometown_location);
		      preparedStatement.setString(6, religion);
		      preparedStatement.setString(7, education);
		      preparedStatement.setString(8, work_history);
		      preparedStatement.setString(9, significant_other);
		      preparedStatement.setString(10, relationship_status);
		      preparedStatement.executeUpdate();
		      System.out.println("hererrrrrrrrrrrrrrrrreeeeeeeeeeeeeee");
	      }
	    } catch (Exception e) {
	      throw e;
	    }  
  }
  
  public void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }

      if (statement != null) {
        statement.close();
      }

      if (connect != null) {
        connect.close();
      }
    } catch (Exception e) {

    }
  }

} 
