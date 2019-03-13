/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.*;

/**
 *
 * @author quanglbm2
 */
public class MiddleProcess {

    /**
     * Currently we only get 1 cluster --> need to implements getting more
     * cluster based on lower bound and upper bound
     */
    public static void main(String[] args) throws Exception {
        System.out.println("3rd-party process is running...");

        // Read cluster        
        HashMap<Integer, ArrayList<ClusterItem>> hashmap = kHSimHelper.readFileFromLocalHash(kHSimHelper.PATH + "/res/result");
        if (hashmap == null) {
            System.out.println("Error, cannot read result file");
            System.exit(1);
        }

        // Read query
        String query = kHSimHelper.readQuery(kHSimHelper.PATH + "/input/query.txt");
        if (query == null || query.isEmpty()) {
            System.out.println("Error, cannot read query file.");
            System.exit(1);
        } else {
            System.out.println("Query = " + query);
        }

        // Get Shingle from query		
        String queryShingle = kHSimHelper.genShingle(query);

        // Find cluster for query
        String kQueryShingle[] = queryShingle.split(";");
        double LB = kHSimHelper.findLB(kQueryShingle.length, kHSimHelper.DEFAULT_EPSILON);
        double UB = kHSimHelper.findUB(kQueryShingle.length, kHSimHelper.DEFAULT_EPSILON);
        int[] queryClusters = kHSimHelper.findRange(LB, UB);
        System.out.print("Query belongs to clusters: ");
        
        String listOfCluster = "";
        for (int i : queryClusters) {
            listOfCluster = listOfCluster + i + ";";
            System.out.print(i + " ");
        }
        System.out.println("\n");

        // Find random K element from cluster and compute their similarity
        ArrayList<ClusterItem> chosenCluster = kHSimHelper.getAllItemsFromCluster(hashmap, queryClusters);  // FIX TODO
        if (chosenCluster == null) {
            System.out.println("Error, cannot read result file");
            System.exit(1);
        }
        boolean isSatisfy = false;
        int stepCount = hashmap.size() + 1;                     //  stepcount to prevent infinite loop
        ClusterItem largestItem = new ClusterItem();
        ClusterItem smallestItem = new ClusterItem();

        while (isSatisfy == false && stepCount >= 0) {
            ClusterItem[] chosenItems = kHSimHelper.getKRandomItem(chosenCluster, kHSimHelper.K);
            ArrayList<ClusterItem> calculatedItems = new ArrayList<>();
            for (ClusterItem item : chosenItems) {
                double similarityScore = kHSimHelper.calculateSim(item.getSh(), queryShingle);
                ClusterItem calItem = item;
                calItem.setSimilarityScore(similarityScore);
                calculatedItems.add(calItem);
            }

            // Sorting their similary and get the largest one as epsilon
            Collections.sort(calculatedItems);
            largestItem = calculatedItems.get(calculatedItems.size() - 1);
            smallestItem = calculatedItems.get(0);

            if (largestItem != null && largestItem.getSimilarityScore() >= kHSimHelper.THRESHOLD) {
                isSatisfy = true;
            }
            stepCount--;
        }

        // Using epsilon to find threshold in chosen cluster, then pass to a file for MR-2
        // to calculate similarity for all object within these threshold
        double epsilon1 = largestItem.getSimilarityScore();
        double epsilon2 = smallestItem.getSimilarityScore();

        //  Write result to file; line1 = epsilon1; line2 = cluster_id;cluster_id  ;
        kHSimHelper.writeToFile("res/midProcessRes", queryShingle, epsilon1, listOfCluster, epsilon2);
        kHSimHelper.writeToFile("/src/pathMR2", "data/input/midProcessRes" + "\n" + "data/input/result");
        System.out.println("Calculated epsilon and written to midProcessRes");

        // Consider: keep the file running, continuing looking for changes in some file
        // When we want to trigger 3rd-party process, just change the file
    }

    
}
