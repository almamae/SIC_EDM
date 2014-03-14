package JavaToDB;

import java.io.*;
import java.util.*;
import java.lang.*;
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Alma mae
 */


public class ConcensusFetcher {
      public void seekConsensus() throws IOException{
    FileHandler fh = new FileHandler();

    File fLister = new File("C:/Users/RocheLee/Documents/ROCHE/ACADS/2013-2014 2nd sem/CMSC 198.2 - SP/Codes/SET 8 classified Data/ALMA_MAE/set 8");
    ArrayList<String> files = fh.listFilesForFolder(fLister);

    ArrayList<String> statList1 = new ArrayList<String>();
    ArrayList<String> statList2 = new ArrayList<String>();

    /*File fLister1 = new File("C:/Users/RocheLee/Documents/ROCHE/ACADS/2013-2014 2nd sem/CMSC 198.2 - SP/Codes/SET 8 classified Data/ROCHE/set 8");
    ArrayList<String> files = fh.listFilesForFolder(fLister1);*/
    String filename, uid;

    for(int i=0; i<files.size();i++){
      filename = files.get(i).substring(files.get(i).lastIndexOf('\\')+1, files.get(i).length());
      uid = filename.substring(0,filename.lastIndexOf('_'));
      System.out.println(uid);

      statList1 = fh.readIdentifiedFile(files.get(i));//alma
      statList2 = fh.readIdentifiedFile("C:/Users/RocheLee/Documents/ROCHE/ACADS/2013-2014 2nd sem/CMSC 198.2 - SP/Codes/SET 8 classified Data/ROCHE_LEE/set 8/"+uid+"_Rclassifiedstat.txt");//roche
      //fh.loadStatLists("ako_Rclassifiedstat.txt","ako_Aclassifiedstat.txt");

      ArrayList<String> consensusStats = compare(statList1, statList2);
      fh.writeFile("C:/Users/RocheLee/Documents/ROCHE/ACADS/2013-2014 2nd sem/CMSC 198.2 - SP/Codes/SET 8 classified Data/CONSENSUS/set 8/"+uid+"_consensus_stats.txt", consensusStats);

    }
  }

  public ArrayList<String> compare(ArrayList<String> statList1, ArrayList<String> statList2){
    ArrayList<String> consensusStats = new ArrayList<String>();

    for(int i=0;i<statList1.size();i++){
      String[] s1 = statList1.get(i).split("=>>");
      String[] type1 = s1[1].trim().split(":");
      if(type1.length>1){
        Integer.valueOf(type1[1].trim());
      }

      String[] s2 = statList2.get(i).split("=>>");
      String post[] = s2[0].trim().split(":"); //split to get postID

      String[] type2 = s2[1].trim().split(":");
      //Integer.valueOf(type2[1].trim());
      if(type2.length>1){
        Integer.valueOf(type2[1].trim());

        if(Integer.valueOf(type1[1].trim())==Integer.valueOf(type2[1].trim())){ //consensus achieved
            //add status as consensus Stats item
            //String str = statList1.get(i).getStatID() + "," + statList1.get(i).getEmotype();
            consensusStats.add(post[0] + ";" + type2[1]);
        }
      }

      //int otherIndex = searchStat(statList1.getItem(i).getStatID(), statList2); //assumed to never be equal to -1, refer to searchStat()
    }

    return consensusStats;
  }
  
  public ArrayList<String> getConcensusPerPerson(String uid){
      ArrayList<String> concensusStatList = new ArrayList();
      String directory = "F:\\CONSENSUS\\set 8";
      String filename = uid;
      filename += "_indirect_output_consensus_stats.txt";
      System.out.println(filename);
      FileHandler fh = new FileHandler();
      try{
          concensusStatList = fh.readIdentifiedFile("F:\\CONSENSUS\\set 8\\"+filename);
      } catch (IOException e) {
      
      }
      
      return concensusStatList;
  }
  
}
