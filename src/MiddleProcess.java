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

    public static int K = 2;
    public static int THRESHOLD = 10;
    public static int SHINGLE_LENGTH = 2;

    public static void main(String[] args) throws Exception {
        System.out.println("3rd-party process is running...");

        // Read cluster        
        HashMap<Integer, ArrayList<ClusterItem>> hashmap = readFileFromLocalHash("/home/minhquang/Documents/hadoop_demo1/res/result");
        if (hashmap == null) {
            System.out.println("Error, cannot read result file");
            System.exit(1);
        }

        // Read query
        String query = readQuery("/home/minhquang/Documents/hadoop_demo1/input/query.txt");
        if (query == null || query.isEmpty()) {
            System.out.println("Error, cannot read query file.");
            System.exit(1);
        } else {
            System.out.println("Query = " + query);
        }

        // Get Shingle from query		
        String queryShingle = new Shingle().genShingle(query);

        // Find cluster for query
        String kQueryShingle[] = queryShingle.split(";");
        int queryCluster = (int) (Math.floor(kQueryShingle.length / SHINGLE_LENGTH + 1));       //  need modified
        System.out.println("Query belongs to cluster " + queryCluster);

        // Find random K element from cluster and compute their similarity
        ArrayList<ClusterItem> chosenCluster = hashmap.get(queryCluster);
        if (chosenCluster == null) {
            System.out.println("Error, cannot read result file");
            System.exit(1);
        }
        boolean isSatisfy = false;
        int stepCount = hashmap.size() + 1;                     //  stepcount to prevent infinite loop
        ClusterItem largestItem = new ClusterItem();
        ClusterItem smallestItem = new ClusterItem();

        while (isSatisfy == false && stepCount >= 0) {
            ClusterItem[] chosenItems = getKRandomItem(chosenCluster, K);
            ArrayList<ClusterItem> calculatedItems = new ArrayList<>();
            for (ClusterItem item : chosenItems) {
                int similarityScore = new Shingle().compare(item.getSh(), queryShingle);
                ClusterItem calItem = item;
                calItem.setSimilarityScore(similarityScore);
                calculatedItems.add(calItem);
            }

            // Sorting their similary and get the largest one as epsilon
            Collections.sort(calculatedItems);
            largestItem = calculatedItems.get(calculatedItems.size() - 1);
            smallestItem = calculatedItems.get(0);

            if (largestItem != null && largestItem.getSimilarityScore() >= THRESHOLD) {
                isSatisfy = true;
            }
            stepCount--;
        }

        // Using epsilon to find threshold in chosen cluster, then pass to a file for MR-2
        // to calculate similarity for all object within these threshold
        int epsilon1 = largestItem.getSimilarityScore();
        int epsilon2 = smallestItem.getSimilarityScore();

        //  Write result to file; line1 = epsilon1; line2 = cluster_id;cluster_id  ;
        writeToFile("/home/minhquang/Documents/hadoop_demo1/res/midProcessRes", queryShingle, epsilon1, String.valueOf(queryCluster), epsilon2, String.valueOf(queryCluster));
        writeToFile("/home/minhquang/Documents/hadoop_demo1/src/pathMR2", "data/input/midProcessRes" + "\n" + "data/input/result");
        System.out.println("Calculated epsilon and written to midProcessRes");

        // Consider: keep the file running, continuing looking for changes in some file
        // When we want to trigger 3rd-party process, just change the file
    }

    public static ClusterItem[] readFileFromLocal(String path) {
        ArrayList<ClusterItem> res = new ArrayList<>();
        try {
            File file = new File(path);
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                res.add(convertToClusterItem(sc.nextLine()));
            }
        } catch (FileNotFoundException e) {
        }
        return res.toArray(new ClusterItem[res.size()]);
    }

    public static HashMap<Integer, ArrayList<ClusterItem>> readFileFromLocalHash(String path) {
        HashMap<Integer, ArrayList<ClusterItem>> hashmap = new HashMap<>();
        try {
            File file = new File(path);
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                ClusterItem item = convertToClusterItem(sc.nextLine());
                ArrayList<ClusterItem> list = new ArrayList<>();
                if (hashmap.get(item.getClusterId()) != null) {
                    list = hashmap.get(item.getClusterId());
                }
                list.add(item);
                hashmap.put(item.getClusterId(), list);
            }
        } catch (FileNotFoundException e) {
        }
        return hashmap;
    }

    public static String readQuery(String path) {
        try {
            File file = new File(path);
            Scanner sc = new Scanner(file);
            if (sc.hasNextLine()) {
                return sc.nextLine();
            }
            return null;
        } catch (FileNotFoundException e) {
        }
        return null;
    }

    public static ClusterItem convertToClusterItem(String s) {
        ClusterItem item = new ClusterItem();
        String[] res1 = s.split("	");
        item.setClusterId((int) Double.parseDouble(res1[0]));

        String[] res2 = res1[1].split("@");
        item.setUrl(res2[0]);
        item.setNos(Integer.parseInt(res2[1]));
        item.setSh(res2[2]);

        return item;
    }

    private static ClusterItem[] getKRandomItem(ArrayList<ClusterItem> chosenCluster, int K) {
        ArrayList<ClusterItem> res = new ArrayList<>();
        int maxLimit = chosenCluster.size();
        int count = K;
        while (count != 0) {
            int n = new Random().nextInt(maxLimit);
            if (res.contains(chosenCluster.get(n))) {
                continue;
            }
            res.add(chosenCluster.get(n));
            count--;
        }
        return res.toArray(new ClusterItem[res.size()]);
    }

    // private static void writeToFile(String path, int epsilon1, String listOfCluster, int epsilon2, String listOfCluster2) {
    //     try {
    //         String fileContent = epsilon1 + "\n" + listOfCluster + "\n" + epsilon2 + "\n" + listOfCluster2;
    //         Files.write(Paths.get(path), fileContent.getBytes());
    //     } catch (Exception e) {
    //     }
    // }

    private static void writeToFile(String path, String query, int epsilon1, String listOfCluster, int epsilon2, String listOfCluster2) {        
        writeToFile(path, query + "\n" + epsilon1 + "\n" + listOfCluster + "\n" + epsilon2 + "\n" + listOfCluster2);
    }

    private static void writeToFile(String path, String fileContent) {
        try {            
            Files.write(Paths.get(path), fileContent.getBytes());
        } catch (Exception e) {
        }
    }
}
