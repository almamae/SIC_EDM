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
    
    public static void main(String[] args){
        ConcensusFetcher concensusFetcher = new ConcensusFetcher();
        try {
            concensusFetcher.seekConsensus("F:/ALMA CLASSIFIED FILES/set 6", "F:/ROCHE CLASSIFIED FILES/set6");
        } catch(IOException e) {
           System.out.println("Error in file handling.");
        }
    }
    public void seekConsensus(String almaDirectory, String rocheDirectory) throws IOException{
        FileHandler fh = new FileHandler();

        File fLister = new File(almaDirectory);
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
            statList2 = fh.readIdentifiedFile(rocheDirectory+"/"+uid+".txt_Rclassifiedstat.txt");//roche
            //fh.loadStatLists("ako_Rclassifiedstat.txt","ako_Aclassifiedstat.txt");
            System.out.println(statList1.size() + " " + statList2.size());
            if(statList1.size() != statList2.size()){
                fh.writeFile(files.get(i).replaceAll("set 6", "set 6_1"), statList1);
                fh.writeFile(rocheDirectory.replaceAll("set6", "set 6_1/")+uid+"_Rclassifiedstat.txt", statList2);
            }
            else {
                ArrayList<String> consensusStats = compare(statList1, statList2);
                fh.writeFile("F:/CONSENSUS/set 6/"+uid+"_consensus_stats.txt", consensusStats);
            }

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
  
  public ArrayList<String> getConcensusPerPerson(String filename){
      ArrayList<String> concensusStatList = new ArrayList();
      filename += "_consensus_stats.txt";
      System.out.println(filename);
      FileHandler fh = new FileHandler();
      try{
          concensusStatList = fh.readIdentifiedFile("D:\\CONSENSUS\\set 4\\"+filename);
      } catch (IOException e) {
      
      }
      
      return concensusStatList;
  }
  
}
