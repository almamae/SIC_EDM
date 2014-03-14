package JavaToDB;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DBSaver {
	
	public static void main(String[] args)throws IOException, Exception{
		DBSaver dbs = new DBSaver();
		FileHandler fh = new FileHandler();
		
		ArrayList<ArrayList<String>> postList = new ArrayList<ArrayList<String>>();
//		ArrayList<String> likesList =  new ArrayList<String>();
//		ArrayList<String> groupsList =  new ArrayList<String>();
//		ArrayList<String> activeFriendsList =  new ArrayList<String>();
//		ArrayList<String> basicInfoList =  new ArrayList<String>();
		

		ArrayList<String> files = fh.listFilesForFolder(new File("F:/SP PROCESSED DATA/processed/feb 20"));
                String filename;

                DatabaseHandler dbh = new DatabaseHandler();
                dbh.openDBconn();
                String directoryOfSaved = "F:\\savedData";
                for(int i=0;i<files.size();i++){
                    filename = files.get(i).substring(files.get(i).lastIndexOf('\\')+1, files.get(i).length());
                    String uid = filename.substring(0,filename.indexOf('_'));
                    System.out.println(uid);
                    fh = new FileHandler();
                    fh.readFile(files.get(i));
                    
                    //System.out.println(files.get(i));
                    postList = fh.getPostList();
                    //process post then save to db
                    if(postList.size()>0){
                        System.out.println("has posts");
                        dbs.savePostToDB(postList, dbh, uid);
                    }
/*
                    likesList = fh.getLikesList();
                    //process likes then save to db
                    if(groupsList.size()>0){
                            dbs.saveLikesListtoDB(uid, likesList,dbh);
                    }
                    groupsList = fh.getGroupsList();
                    //process groups then save to db
                    if(groupsList.size()>0){
                            dbs.saveGroupListtoDB(uid, groupsList,dbh);
                    }
                    activeFriendsList = fh.getActiveFriendsList();
                    //process friends then save to db
                    if(activeFriendsList.size()>0){
                            dbs.saveActiveFriendstoDB(uid, activeFriendsList,dbh);
                    }

        		basicInfoList = fh.getBasicInfoList();
                    //process basic info then save to db
                    if(basicInfoList.size()>0){
                            dbs.saveBasicInfotoDB(basicInfoList, uid, dbh);
                    }*/
        		
                    if(new File(files.get(i)).renameTo(new File(directoryOfSaved+"\\"+filename))){
                        System.out.println(filename + "  is successfully moved.");
                    } else {
                        System.out.println("File not moved.");
                    }
            }

                    /*System.out.println(postList.size());
                    for(int i=0; i<postList.size();i++){
                            System.out.println(i);
                            for(int j =0; j<postList.get(i).size();j++){
                                    System.out.println("\t"+postList.get(i).get(j));
                            }
                    }*/

            dbh.close();
		
	}
	
	public void saveActiveFriendstoDB(String uid, ArrayList<String> friendslist, DatabaseHandler dbh) throws IOException, Exception{
		
		System.out.println("was hrere");
		for(int j=0; j<friendslist.size();j++){
			System.out.println("\t\t\t"+friendslist.get(j)+"huhuehue");
			if(friendslist.get(j)!=null || !friendslist.get(j).equals("")){
				dbh.insertDBActiveFriends(uid, friendslist.get(j));
			}
			
		}
	}
	
	public void saveGroupListtoDB(String uid, ArrayList<String> groupslist, DatabaseHandler dbh) throws IOException, Exception{

		for(int j=0; j<groupslist.size();j++){
			if(groupslist.get(j)!=null && groupslist.get(j).startsWith("id")){
				String[] s = groupslist.get(j).split("\t");
				System.out.println(s[1] + "jhhsds");
				dbh.insertDBgroups(uid,s[1].trim());
				dbh.insertDBgroupmembership(uid,s[1]);
			}
		}
	}
	
	public void saveLikesListtoDB(String uid, ArrayList<String> likeslist, DatabaseHandler dbh) throws IOException, Exception{

		for(int j=0; j<likeslist.size();j++){
			if(likeslist.get(j)!=null){
				System.out.println(likeslist.get(j));//to be saved page
				dbh.insertDBlikePages(uid,likeslist.get(j).trim());
			}
		}
	}
	
	public void saveBasicInfotoDB(ArrayList<String> info, String uid, DatabaseHandler dbh) throws IOException, Exception{
		String gender = new String();
		StringBuilder languages = new StringBuilder();
		String current_location = new String();
		String hometown_location = new String();
		String religion = new String();
		StringBuilder education = new StringBuilder();
		StringBuilder work_history = new StringBuilder();
		String significant_other_id = new String();
		String relationship_status = new String();
		String line;
		for(int i=0; i < info.size(); i++){
			line = info.get(i);
			String[] tokens = line.split("\t");
			if(tokens[0].equals("gender")){
				gender = tokens[1];
			} else if(tokens[0].equals("languages")) {
				i++;			
				System.out.println(info.size());
	
				do{
					line = info.get(i);
					String[] l = line.split("\t");
					languages.append(l[2]);
                                        languages.append(";");
					System.out.println(l[2]);
					i++;
				}while(i < info.size() && !line.equals(""));
			} else if(tokens[0].equals("school")) {
				education.append(tokens[2]+";");
			} else if(tokens[0].equals("hometown")) {
				hometown_location = tokens[2];
			} else if(tokens[0].equals("location")) {
				current_location = tokens[2];
			} else if(tokens[0].equals("religion")) {
				religion = tokens[1];
			} else if(tokens[0].equals("employer")){
				work_history.append(tokens[2]+";");
			} else if(tokens[0].equals("significant_other")){
				significant_other_id = tokens[1];
			} else if(tokens[0].equals("relationship_status")){
				relationship_status = tokens[1];
			}
		}
		
		dbh.insertDBBasicInfo(uid, gender, languages.toString(), current_location, hometown_location, religion, education.toString(), work_history.toString(), significant_other_id, relationship_status);
	}

	public void savePostToDB(ArrayList<ArrayList<String>> postList, DatabaseHandler dbh, String currentUser)throws IOException, Exception{
            String message;
            String created_time;
            String post_id;
            ArrayList<String> likes;
            ArrayList<String> comments_people;
            ArrayList<String> post;
            ArrayList<String> comment_times;
            String attribute = new String();
            String uid = new String();
            boolean hasMessage;
            ConcensusFetcher cf = new ConcensusFetcher();
            ArrayList<String> concensusStats = cf.getConcensusPerPerson(currentUser);
            for(int i=0; i < postList.size(); i++)
            {
                post = postList.get(i);
                message = new String();
                created_time = new String();
                post_id = new String();
                likes = new ArrayList();
                comments_people = new ArrayList();
                comment_times = new ArrayList();
                hasMessage = false;
                for(int j=0; j < post.size(); j++) {                                
                    attribute = post.get(j);

                    if(attribute.equals("")){
                        j++;
                    } else {
                        String[] line = attribute.split("\t");
                        if(line[0].equals("message")){
                            hasMessage = true;
                            message = getMessage(line);
                        } else if(line[0].equals("created_time")) {
                                created_time = line[1];
                        } else if(line[0].equals("comments")) {
                            while(j+1 < post.size() && !attribute.equals("") ){
                                j++;
                                if(j == post.size())
                                {
                                    System.out.println(post.size());
                                    j--;
                                    break;
                                }
                                String[] time = post.get(j).split("\t");
                                comment_times.add(toDateTimeFormat(time[2]));
                                j++;						
                                String[] from = post.get(j).split("\t");
                                comments_people.add(from[3]);
                                attribute = post.get(j);
                                j++;
                            } 
                        } else if(line[0].equals("likes")){
                            String[] likes_people = attribute.split("\t");
                            for(int l=1; l < likes_people.length; l++){
                                    likes.add(likes_people[l]);
                            }
                        } else if(line[0].equals("post_id")){
                            post_id = line[1];
                        } else if(line[0].equals("from")){
                            uid = line[1];
                        }                          
                    }
                }         

                if(hasMessage) {			
                    String emotype = isConcensus(post_id, concensusStats);
                    System.out.println("post id: " + post_id);

                    System.out.println("emotype: " + emotype);                    
//                    insert status in db      
                    insertStatusDB(post_id, toDateTimeFormat(created_time), comments_people, likes, comment_times, uid, dbh, message, emotype);                    
                } else {
                    insertPostDB(post_id, toDateTimeFormat(created_time), comments_people, likes, comment_times, uid, dbh);
                }
            }
	}   
        
        public void insertPostDB(String post_id, String created_time, ArrayList<String> user_comments, ArrayList<String> likes,
                                ArrayList<String> comment_dates, String userID, DatabaseHandler dbh) throws Exception{
            dbh.insertDBPost(post_id, userID, created_time);
            for(int i=0; i < user_comments.size(); i++){
                dbh.insertDBPostComment(post_id, user_comments.get(i), comment_dates.get(i));
            }
            for(int n = 0; n < likes.size(); n++) {
                dbh.insertDBPostLikes(post_id, likes.get(n));
            }
        }
	
        public void insertStatusDB(String post_id, String created_time, ArrayList<String> user_comments, ArrayList<String> likes,
                                ArrayList<String> comment_dates, String userID, DatabaseHandler dbh, String message, String emotype) throws Exception{
           
            dbh.insertDBStatus(post_id, message, userID, created_time, emotype);
  //        insert comments for status
            for(int i=0; i < user_comments.size(); i++){
                dbh.insertDBStatusComment(post_id, user_comments.get(i), comment_dates.get(i));
            }
//           insert likes for status
            for(int i=0; i < likes.size(); i++){
                dbh.insertDBStatusLike(post_id, likes.get(i));
            } 
        }
        
        public String getMessage(String[] line){
            return "";
        }
	public String toDateTimeFormat(String createdTime){
		Date ct;
		String formattedDate = new String();
                System.out.println(createdTime);
		
		try 
		{
		    ct = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS")
		        .parse(createdTime.replaceAll("=>", ":"));
		    SimpleDateFormat newDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:MM:SS");
		     formattedDate= newDateFormat.format(ct);
                     System.out.println(formattedDate);
		} catch (Exception e) {
			System.out.println("error! in time formatting");
		}
		
		return formattedDate;
	}
        
        public String isConcensus(String post, ArrayList<String> concensusStats){
            String postID;
            String stat;
            for(int i=0; i < concensusStats.size(); i++){
                stat = concensusStats.get(i);
                postID = stat.substring(0,stat.indexOf(";"));

                if(post.equals(postID)){
                    return stat.substring(stat.length()-1);
                }
            }

            return "-1";         
        }
}
