package JavaToDB;

import java.util.*;
import java.io.IOException;

public class SIC_EDM {
	public static void main(String args[]) throws IOException, Exception {
		SIC_EDM sic = new SIC_EDM();
		
		DatabaseHandler dbh = new DatabaseHandler();
		ArrayList<String> posts = new ArrayList<String>();
		ArrayList<String> receivers = new ArrayList<String>();
		
		String[] direct_users = {"100000454245510", "1481855980", "1843306040", "1704122032", "100000226756314", "100003869864847", "741339630", "100002515892541", "100000578448508", "1167052315"};//declare array of direct users;
			
		dbh.openDBconn();
		for(int i=0;i<10;i++){	
			posts = new ArrayList<String>();
			posts = dbh.getPostsfromOCTtoDEC(direct_users[i]); //query post from october to december 2013
			//for each post query receivers
				//monitor the number of receivers diffused and not diffused based on their status emotype
				//for each receiver, 
					//check if there sender and receiver's SIC probability is already taken
						//if true, skip
						//else, get SIC probability, then save to DB
				//save efficiency of SIC-based Model for this post to DB based on diffusibility
			
			for(int j=0;j<posts.size();j++){
				Double diffused_pred = 0.0, notdiffused_pred = 0.0;
				Double curr_prob = 0.0, actualDiffusedRate=0.0, predictedDiffusedRate=0.0, efficiencyRate = 0.0;
				
				String[] s = posts.get(j).split(";");//format: postID;emotype;created_time
				receivers = new ArrayList<String>();
				receivers = dbh.getReceivers(s[0]);//get receivers by postID
				actualDiffusedRate = sic.getActualPercentOfDiffused(dbh,posts.get(j), receivers);
				
				for(int k=0; k<receivers.size();k++){					
					//if(dbh.checkIfProbSaved(direct_users[i],s[0], s[2].trim())==null){
					curr_prob = sic.getSICProb(dbh, direct_users[i].trim(), s[0].trim(), s[2].trim());
					dbh.insertSICprob(direct_users[i], s[0],curr_prob, s[2].trim());//save to DB
					if(curr_prob>50.0){
						diffused_pred++;
					}else{
						notdiffused_pred++;
					}
					/*}else{
						if(dbh.checkIfProbSaved(direct_users[i],s[0])>50.0){
							diffused_pred++;
						}else{
							notdiffused_pred++;
						}
					//}*/
				}
				predictedDiffusedRate = diffused_pred/(diffused_pred + notdiffused_pred);
				if(actualDiffusedRate >= predictedDiffusedRate){
					efficiencyRate = 100-(actualDiffusedRate-predictedDiffusedRate);
				}else{
					efficiencyRate = 100-(predictedDiffusedRate-actualDiffusedRate);
				}
				dbh.insertactualandpredictedSICprob(direct_users[i], s[0], actualDiffusedRate, predictedDiffusedRate, efficiencyRate);//save to db actual, predicted, postID, uid
			}
		}
		dbh.close();
		
	}
	
	public double getSICProb(DatabaseHandler dbh, String uid, String friendID, String timeLimit)throws IOException, Exception{ //not weighted SIC-based Probability	
		double sicprob = 0.0;
		
		sicprob += getSimilarityFactor(dbh, uid, friendID);
		sicprob += getInteractivityFactor(dbh,uid,friendID,timeLimit);
		sicprob += getConnectivityFactor(dbh,uid,friendID,timeLimit);
		
		return sicprob/3;
	}
	
	public double getSimilarityFactor(DatabaseHandler dbh, String uid, String friendID)throws IOException, Exception{
		double sim_prob=0.0;
		//query from database similarities
		sim_prob += dbh.getLikedPageSimilarityFactor(uid, friendID);
		sim_prob += dbh.getFriendsSimilarityFactor(uid, friendID);
		sim_prob += dbh.getGroupsSimilarityFactor(uid, friendID);
		//insert basic_info similarity here
		sim_prob += dbh.getbasicInfoSimilarityFactor(uid, friendID);
		return sim_prob/4;		
	}
	
	public double getInteractivityFactor(DatabaseHandler dbh, String uid, String friendID, String timeLimit)throws IOException, Exception{
		double int_prob=0.0;
		//query from database interactions
		int_prob += dbh.getStatusLikesInteractivity(uid, friendID, timeLimit);
		int_prob += dbh.getStatusCommentsInteractivity(uid, friendID, timeLimit);
		int_prob += dbh.getPostsLikesInteractivity(uid, friendID, timeLimit);
		int_prob += dbh.getPostsCommentsInteractivity(uid, friendID, timeLimit);
		
		return int_prob/2;		
	}
	
	public double getConnectivityFactor(DatabaseHandler dbh, String uid, String friendID, String timeLimit)throws IOException, Exception{
		double con_prob=0.0;
		//query from database interactions
		con_prob = dbh.getConnectivity(uid,friendID, timeLimit);
		
		return con_prob;		
	}

	public double getActualPercentOfDiffused(DatabaseHandler dbh, String post, ArrayList<String> receivers)throws IOException, Exception{
		Double diffusedRate = 0.0;
		Double diffused = 0.0, notdiffused = 0.0;
		/*split post string
		 * query the status of each receiver after the post's created time, get emotype
		 * 		if post_emo==receiver_emo && post_emo!=2(neutral), diffused++
		 * 		else, notDiffused++
		 * */
		
		String[] s = post.split(";");//format: postID;emotype;created_time => s[0]=id;s[1]=emotype;s[2]=created_time
		for(int i=0;i<receivers.size();i++){
			String receiver_emo = dbh.getStatReceiverEmotype(s[0], receivers.get(i), s[2]);
			if(receiver_emo.equals(s[1]) && s[1].equals("2")){
				diffused++;
			}else{
				notdiffused++;
			}
		}
		
		diffusedRate = diffused/(diffused+notdiffused);
		
		return diffusedRate;
	}
}
