/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

/**
 *
 * @author minhquang
 */
public class kHSimHelper {

    public static int LAMDA = 2;
    public static int K = 2;
    public static int THRESHOLD = 10;
    public static double DEFAULT_EPSILON = 0.6;
    public static String PATH = "/home/minhquang/Documents/hadoop_demo1_2/";
    private static final String STOP_SYMBOLS[] = {".", ",", "!", "?", ":", ";", "-", "\\", "/", "*", "(", ")"};

    //  Find correct cluster with provided length
    public static int findCluster(int nOS) {
        return (int) Math.floor((double) nOS / LAMDA + 1);
    }

    //  Find lower bound
    public static double findLB(double nOS, double epsilon) {
        return (nOS * epsilon) / LAMDA;
    }

    //  Find upper bound
    public static double findUB(double nOS, double epsilon) {
        return nOS / (LAMDA * epsilon) + 1;
    }

    //  Find integer between 2 bound
    public static int[] findRange(double lower, double upper) {
        //  Find integer bound
        int intLower = (int) Math.ceil(lower);
        int intUpper = (int) Math.floor(upper);

        int diff = intUpper - intLower;

        switch (diff) {
            case 0: {
                int[] res = {intUpper};
                return res;
            }
            case 1: {
                int[] res = {intLower, intUpper};
                return res;
            }
            default: {
                int[] res = new int[diff + 1];
                int j = 0;
                for (int i = intLower; i <= intUpper; i++) {
                    res[j] = i;
                    j++;
                }
                return res;
            }
        }
    }

    //  Remove unnecessary word
    private static String canonize(String str) {
        for (String stopSymbol : STOP_SYMBOLS) {
            str = str.replace(stopSymbol, "");
        }
        return str.trim();
    }

    //  Generate shingle, output: shingle number + hashcode
    public static String genShingle(String strNew) {
        String str = canonize(strNew.toLowerCase());
        String words[] = str.split(" ");
        int shinglesNumber = words.length - LAMDA + 1;
        String shingles = "";

        //Create all shingles 
        for (int i = 0; i < shinglesNumber; i++) {
            String shingle = "";

            //Create one shingle 
            for (int j = 0; j < LAMDA; j++) {
                shingle = shingle + words[i + j] + " ";
            }

            shingles = shingles + shingle.hashCode() + ";";
        }

        return shinglesNumber + "@" + shingles;
    }

    //  Calculate Similarity Score
    public static double calculateSim(String text1, String text2) {
        //  Catch null
        if (text1.trim().isEmpty() || text2.trim().isEmpty() || text1 == null || text2 == null) {
            return 0;
        }

        String[] shingle1 = text1.split(";");
        String[] shingle2 = text2.split(";");
        double similarShinglesNumber = 0D;

        for (int i = 0; i < shingle1.length; i++) {
            for (int j = 0; j < shingle2.length; j++) {
                if (shingle1[i].equalsIgnoreCase(shingle2[j])) {
                    similarShinglesNumber++;
                }
            }
        }
        return ((similarShinglesNumber / (shingle1.length + shingle2.length - similarShinglesNumber)));
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
                ArrayList<ClusterItem> itemList = readCluster(sc.nextLine());
                if (itemList == null) {
                    return null;
                }
                int clusterId = itemList.get(0).getClusterId();
                hashmap.put(clusterId, itemList);
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

    public static ArrayList<ClusterItem> readCluster(String s) {
        ArrayList<ClusterItem> result = new ArrayList<>();
        String[] res1 = s.split("	");
        int clusterId = (int) Double.parseDouble(res1[0]);

        String[] res2 = res1[1].split("#");
        for (String it : res2) {
            String[] res3 = it.split("@");
            ClusterItem item = new ClusterItem();
            item.setClusterId(clusterId);
            item.setUrl(res3[0]);
            item.setNos(Integer.parseInt(res3[1]));
            item.setSh(res3[2]);

            result.add(item);
        }
        return result;
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

    public static ClusterItem[] getKRandomItem(ArrayList<ClusterItem> chosenCluster, int K) {
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

    public static void writeToFile(String folder, String query, double epsilon1, String listOfCluster, double epsilon2) {        
        writeToFile(PATH + folder, query + "\n" + epsilon1 + "\n" + listOfCluster + "\n" + epsilon2 + "\n" + listOfCluster);
    }

    public static void writeToFile(String path, String fileContent) {
        try {
            Files.write(Paths.get(path), fileContent.getBytes());
        } catch (Exception e) {
        }
    }
    
    public static ArrayList<ClusterItem> getAllItemsFromCluster(HashMap<Integer, ArrayList<ClusterItem>> hashmap, int[] clusterList) {
        ArrayList<ClusterItem> res = new ArrayList<>();
        
        for(int i : clusterList) {
            res.addAll(hashmap.get(i));
        }
        
        return res;
    }
}
