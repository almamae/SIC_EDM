/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JavaToDB;
import java.io.*;
import java.util.*;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

/**
 *
 * @author Roche Lee
 */
public class FileHandler {
	private ArrayList<ArrayList<String>> postList = new ArrayList<ArrayList<String>>();
	private ArrayList<String> likesList = new ArrayList<String>();
	private ArrayList<String> groupsList = new ArrayList<String>();
	private ArrayList<String> activeFriendsList = new ArrayList<String>();
	private ArrayList<String> basicInfoList = new ArrayList<String>();
	
	/**
	 * @return the postList
	 */
	public ArrayList<ArrayList<String>> getPostList() {
		return postList;
	}

	/**
	 * @return the likesList
	 */
	public ArrayList<String> getLikesList() {
		return likesList;
	}

	/**
	 * @return the groupsList
	 */
	public ArrayList<String> getGroupsList() {
		return groupsList;
	}

	/**
	 * @return the activeFriendsList
	 */
	public ArrayList<String> getActiveFriendsList() {
		return activeFriendsList;
	}

	/**
	 * @return the basicInfoList
	 */
	public ArrayList<String> getBasicInfoList() {
		return basicInfoList;
	}

	/**Reads the content of the source code file then examines and assigns every line to corresponding lists
	 * @param filename of the file to be read
	 * @return void
	 */
	public void readFile(String filename)throws IOException{
		FileReader fr = null;
		BufferedReader br = null;

		String aLine = "";
		String marker = "";
		
		try{
		  fr = new FileReader(filename);
		  br = new BufferedReader(fr);
		  
		  ArrayList<String> post = new ArrayList<String>();
		
		  while((aLine = br.readLine()) !=null){
                      if(aLine.equals("POSTS:") || marker.equals("posts")){ //set marker to post section and save to postList
		    	//System.out.println(aLine);
                        if(aLine.equals("POSTS:")){
		    		marker = "posts";
		    	}else{
			    	if(aLine.equals("END POSTS")){ //end of post section.
			    		marker = "";
			    	}else{
                                    if(aLine.equals("")){//add post
                                        if(!post.isEmpty())
                                            postList.add(post);
                                            post = new ArrayList<String>();
                                    }else{ //add attributes to a post i.e post_id, likes, etc...
                                            post.add(aLine);
                                    }
			    	}
		    	}		    	
		    }else if(aLine.equals("LIKES:") || marker.equals("likes")){ //set marker to likes
		    	if(aLine.equals("LIKES:")){
		    		marker = "likes";
		    	}else{
			    	if(aLine.equals("END LIKES")){ //end of likes section.
			    		marker = "";
			    	}else{
			    		likesList.add(aLine);
			    	}
		    	}		
		    }else if(aLine.equals("GROUPS:") || marker.equals("groups")){ //set marker to groups
		    	if(aLine.equals("GROUPS:")){
		    		marker = "groups";
		    	}else{
			    	if(aLine.equals("END GROUPS")){ //end of groups section.
			    		marker = "";
			    	}else if(aLine.contains("id")){
			    		groupsList.add(aLine);
			    	}
		    	}		
		    }else if(aLine.equals("ACTIVE FRIENDS:") || marker.equals("friends")){ //set marker to friends
		    	if(aLine.equals("ACTIVE FRIENDS:")){
		    		marker = "friends";
		    	}else{
			    	if(aLine.equals("END ACTIVE FRIENDS")){ //end of active friends section.
			    		marker = "";
			    	}else{
			    		activeFriendsList.add(aLine);
			    	}
		    	}		
		    }else if(aLine.equals("BASIC INFO:") || marker.equals("basic")){ //set marker to basic
		    	if(aLine.equals("BASIC INFO:")){
		    		marker = "basic";
		    	}else{
			    	if(aLine.equals("END BASIC INFO")){ //end of basic info section.
			    		marker = "";
			    	}else{
			    		basicInfoList.add(aLine);
			    	}
		    	}		
		    }
		  }//end of while loop
		}catch (FileNotFoundException e) {
		     System.out.println("File not found!");
		     e.printStackTrace();
		     throw e;
		}catch (IOException e) {
		     System.out.println("IO Error!");
		     e.printStackTrace();
		     throw e;
		} finally {
			if(fr != null){
				fr.close();
			}
			if(br != null){
			    br.close();
			}
		}
        }
        	
	public ArrayList<String> listFilesForFolder(final File folder) {
	    ArrayList<String> files = new ArrayList<String>();
	    String filename;
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            //listFilesForFolder(fileEntry);
	        } else {
	          filename = fileEntry.getAbsolutePath();
	          files.add(filename);
	        }
	    }
	    return files;
	}
        
   public ArrayList<String> readIdentifiedFile(String filename)throws IOException{
      FileReader fr = null;
      BufferedReader br = null;

      String aLine = new String();
//      boolean statSection=false;

      ArrayList<String> inputList = new ArrayList();

      try{
       fr = new FileReader(filename);
       br = new BufferedReader(fr);

       while((aLine = br.readLine()) !=null){
        inputList.add(aLine);
       }

     }catch (FileNotFoundException e) {
          System.out.println("File not found!");
          e.printStackTrace();
          throw e;
       }catch (IOException e) {
          System.out.println("IO Error!");
          e.printStackTrace();
          throw e;
        } finally {
       if(fr != null){
         fr.close();
       }
       if(br != null){
         br.close();
       }
      }
      return inputList;
  }
   
     public boolean writeFile(String outputFilename, List<String> inputList) throws IOException{
       FileWriter fw = null;
       BufferedWriter bw = null;
       String line = "";

       try{
         fw = new FileWriter(outputFilename);
         bw = new BufferedWriter(fw);
         for(String input : inputList){
           bw.write(input);
           bw.newLine();
         }
         return true;
       }catch (IOException e){
         //System.out.println("Error!");
         //e.printStackTrace();
         return false;
       }finally{
         if(bw!=null){
           bw.close();
         }
         if(fw!=null){
           fw.close();
         }
       }

   }

}
