/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.util.ArrayList;

/**
 *
 * @author minhquang
 */
public class kHSimHelper {

    public static int LAMDA = 2;
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
    public static int calculateSim(String text1, String text2) {
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
        return (int) ((similarShinglesNumber / (shingle1.length + shingle2.length - similarShinglesNumber)) * 100);
    }
}
