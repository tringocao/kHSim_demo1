/**
 * Word based shingle algorithm 
 * 
 * @author sadv1r 
 * 
 * @version 0.8 
 * 
 * 
 */ 
public class Shingle { 
    private static final String STOP_SYMBOLS[] = {".",",","!","?",":",";","-","\\","/","*","(",")"}; 
    private static final String STOP_WORDS[] = {"это", "как", "так", "и", "в", "над", "к", "до", "не", "на", "но", "за", "то", "с", "ли", "а", "во", "от", "со", "для", "о", "же", "ну", "вы", "бы", "что", "кто", "он", "она"}; 
    private static final int SHINGLE_LEN = 2; 
 
    private String canonize(String str) { 
        for (String stopSymbol : STOP_SYMBOLS) { 
            str = str.replace(stopSymbol, ""); 
        } 
 
        for (String stopWord : STOP_WORDS) { 
            str = str.replace(" " + stopWord + " ", " "); 
        } 
 
        return str; 
    } 
 
       public String genShingle(String strNew) { 
        String str = canonize(strNew.toLowerCase()); 
        String words[] = str.split(" "); 
        int shinglesNumber = words.length - SHINGLE_LEN; 
        String shingles = ""; 
 
        //Create all shingles 
 
        for (int i = 0; i <= shinglesNumber; i++) { 
            String shingle = ""; 
 
            //Create one shingle 
            for (int j = 0; j < SHINGLE_LEN; j++) { 
                shingle = shingle + words[i+j] + " "; 
            } 
 
            shingles = shingles + shingle.hashCode() + ";"; 
        } 
 
        return shingles; 
    } 
 
    public int compare(String textShingles1New, String textShingles2New) { 
        //textShingles1New and textShingles2New equals "" or null bug fix 
        if (textShingles1New.equals("")  || textShingles2New.equals("")  || 
            textShingles1New.equals(" ") || textShingles2New.equals(" ") || 
            textShingles1New == null     || textShingles2New == null)       return 0; 
 
        String textShingles1[] = textShingles1New.split(";"); 
        String textShingles2[] = textShingles2New.split(";"); 
 
        int textShingles1Number = textShingles1.length; 
        int textShingles2Number = textShingles2.length; 
 
        int textShingles1Int[] = new int[textShingles1Number]; 
        int textShingles2Int[] = new int[textShingles2Number]; 
 
        for (int i=0; i<textShingles1Number;i++) { 
            textShingles1Int[i] = Integer.parseInt(textShingles1[i]); 
        } 
        for (int i=0; i<textShingles2Number;i++) { 
            textShingles2Int[i] = Integer.parseInt(textShingles2[i]); 
        } 
 
        double similarShinglesNumber = 0; 
 
        for (int i=0;i<textShingles1Number;i++) { 
            for (int j=0;j<textShingles2Number;j++) { 
                if (textShingles1Int[i] == textShingles2Int[j]) similarShinglesNumber++; 
            } 
        } 
 
        return (int) ((similarShinglesNumber / ((textShingles1Number + textShingles2Number) - similarShinglesNumber)) * 100); 
 
 
 
    } 
 
}


