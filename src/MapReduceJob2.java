
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.lang.Math;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import org.apache.hadoop.util.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

/**
 *
 * @author minhquang
 */
public class MapReduceJob2 {

    public static class MapReduceJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {

        static enum CountersEnum {
            NUM_OF_CLUSTER_READ,
            NUM_OF_SIM_CALCULATED,
            NUM_OF_ITEM_READ
        }

        private Configuration conf;
        private BufferedReader fis;
        private Set<String> contentRead = new HashSet<String>();
        public static HashMap<String, ArrayList<ClusterItem>> TOP_K_MAP = new HashMap<String, ArrayList<ClusterItem>>();
        public static HashMap<String, Double> EPSILON_MAP = new HashMap<>();

        public static ArrayList<ClusterItem> TOP_K = new ArrayList<>();
        public static double EPSILON = 0.0D;
        public static boolean IS_INCREMENT = false;
        public static boolean IS_MULTIPLE_QUERY = false;
        public static String QUERY = "";
        public static ArrayList<String> QUERY_LIST = new ArrayList<>();
        private double LB;
        private double UB;
        private String queryShingle;
        public static ArrayList<String> QUERY_SHINGLE_LIST;
        private int queryShingleLength;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            URI[] filesURIs = Job.getInstance(conf).getCacheFiles();
            System.out.println("SIZE = \t" + filesURIs.length);
            for (URI fileURI : filesURIs) {
                Path path = new Path(fileURI.getPath());
                String fileName = path.getName();
                System.out.println("READING\t" + fileName);
                parseFiles(fileName);
            }

            IS_MULTIPLE_QUERY = TOP_K_MAP.size() > 1;

            //  TOP_K_MAP co size > 1: multi-query
            if (IS_MULTIPLE_QUERY) {
                //  Vua multi-query, vua increment
                if (IS_INCREMENT) {
                    Set<Entry<String, ArrayList<ClusterItem>>> entrySet = TOP_K_MAP.entrySet();
                    ArrayList<Entry<String, ArrayList<ClusterItem>>> listOfEntry = new ArrayList<Entry<String, ArrayList<ClusterItem>>>(entrySet);
                    for (Entry<String, ArrayList<ClusterItem>> entry : listOfEntry) {
                        ArrayList<ClusterItem> currentTopK = entry.getValue();
                        Collections.sort(currentTopK);

                        EPSILON_MAP.put(entry.getKey(), currentTopK.get(0).getSimilarityScore());
                    }
                } //  Chi multi-query
                else {
                    EPSILON = kHSimHelper.DEFAULT_EPSILON;

                    //  Gen list query shingle
                    QUERY_SHINGLE_LIST = new ArrayList<>();
                    for (String s : QUERY_LIST) {
                        String shingle = kHSimHelper.uniqueQuery(kHSimHelper.genShingle(s).split("@")[1]);
                        QUERY_SHINGLE_LIST.add(shingle);

                        String kQueryShingle[] = shingle.split(";");

                        //  Tim min LB va max UB
                        double lb = kHSimHelper.findLB(kQueryShingle.length, EPSILON);
                        if (lb < LB) {
                            LB = lb;
                        }

                        double ub = kHSimHelper.findUB(kQueryShingle.length, EPSILON);
                        if (ub > UB) {
                            UB = ub;
                        }
                    }
                }
                //  TOP_K_MAP co size == 1: 1 query 
            } else {
                //  1 query, increment
                if (IS_INCREMENT) {
                    TOP_K = new ArrayList<ClusterItem>();
                    TOP_K = TOP_K_MAP.get(QUERY);

                    Collections.sort(TOP_K);

                    EPSILON = TOP_K.get(0).getSimilarityScore();
                } //  1 query, khong increment (Truong hop binh thuong)
                else {
                    System.out.println("1 QUERY, NO INCREMENT\t" + QUERY);
                    QUERY = QUERY.trim();

                    EPSILON = kHSimHelper.DEFAULT_EPSILON;

                }

                //  Gen shingle query
                queryShingle = kHSimHelper.uniqueQuery(kHSimHelper.genShingle(QUERY).split("@")[1]);

                // Find cluster for query
                String kQueryShingle[] = queryShingle.split(";");
                queryShingleLength = kQueryShingle.length;

                LB = kHSimHelper.findLB(queryShingleLength, EPSILON);
                UB = kHSimHelper.findUB(queryShingleLength, EPSILON);

            }

        }

        private void parseFiles(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String line = null;
                String currentQuery = "";
                while ((line = fis.readLine()) != null) {
                    System.out.println("LINE =\t" + line);

                    String[] res = line.split("\t");

                    if (res.length > 1) {
                        IS_INCREMENT = true;
                        System.out.println("FOUND CLUSTER ITEM");
                        ClusterItem item = new ClusterItem(res[0], Double.parseDouble(res[1]));

                        ArrayList<ClusterItem> nList = TOP_K_MAP.get(currentQuery);
                        nList.add(item);

                        TOP_K_MAP.put(currentQuery, nList);
                    } else {
                        currentQuery = line.trim();
                        System.out.println("FOUND QUERY\t" + currentQuery);

                        TOP_K_MAP.put(currentQuery, new ArrayList<ClusterItem>());

                        QUERY = currentQuery;
                        QUERY_LIST.add(currentQuery);
                    }

                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            //  Get item list
            int clusterId = (int) key.get();

            if (IS_INCREMENT && IS_MULTIPLE_QUERY) {
                MapReduceJob2Helper.processMultiQueryIncrement(context, content, EPSILON_MAP, queryShingle, queryShingleLength, EPSILON, clusterId);
            } else {
                //  Check if clusterId suitable
                boolean isRightCluster = ((double) clusterId >= LB) && ((double) clusterId <= UB);

                if (isRightCluster) {

                    ArrayList<ClusterItem> itemList = kHSimHelper.readCluster(content, clusterId);

                    context.getCounter(CountersEnum.class.getName(), CountersEnum.NUM_OF_CLUSTER_READ.toString()).increment(1);
                    context.getCounter(CountersEnum.class.getName(), CountersEnum.NUM_OF_ITEM_READ.toString()).increment(itemList.size());

                    if (IS_INCREMENT) {
                        MapReduceJob2Helper.processIncrement(context, itemList, queryShingle, EPSILON);
                    } else if (IS_MULTIPLE_QUERY) {
                        ArrayList queryList = kHSimHelper.uniqueQueryList(QUERY_LIST);
                        MapReduceJob2Helper.processMultipleQuery(context, itemList, queryList, queryShingle, queryShingleLength, EPSILON, clusterId);
                    } else {
                        MapReduceJob2Helper.processOneQuery(context, itemList, queryShingle, queryShingleLength, EPSILON, clusterId, "");
                    }
                }
            }
        }
    }

    public static class MapReduceJob2Reduce extends Reducer<Text, Text, Text, Text> {

        ArrayList<ClusterItem> list = new ArrayList<>();
        HashMap<String, ArrayList<ClusterItem>> hashmap = new HashMap<String, ArrayList<ClusterItem>>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double maxScore = 0.0D;
            for (Text similarityScore : values) {
                double sim = Double.parseDouble(similarityScore.toString());
                if (sim > maxScore) {
                    maxScore = sim;
                }
            }
            if (MapReduceJob2Mapper.IS_MULTIPLE_QUERY) {
                String[] s = key.toString().split(",", 2);
                String url = s[0];
                String query = s[1];

                if (hashmap.get(query) == null || hashmap.get(query).isEmpty()) {
                    ArrayList<ClusterItem> nList = new ArrayList<ClusterItem>();
                    nList.add(new ClusterItem(url, query, maxScore));
                    hashmap.put(query, nList);
                } else {
                    ArrayList<ClusterItem> nList = hashmap.get(query);
                    nList.add(new ClusterItem(url, query, maxScore));
                    hashmap.put(query, nList);
                }
            } else {
                list.add(new ClusterItem(key.toString(), MapReduceJob2Mapper.QUERY, maxScore));
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (MapReduceJob2Mapper.IS_MULTIPLE_QUERY) {
                //  Vua multi-query vua increment
                if (MapReduceJob2Mapper.IS_INCREMENT) {
                    Set<Entry<String, ArrayList<ClusterItem>>> entrySet = hashmap.entrySet();
                    ArrayList<Entry<String, ArrayList<ClusterItem>>> listOfEntry = new ArrayList<Entry<String, ArrayList<ClusterItem>>>(entrySet);
                    for (Entry<String, ArrayList<ClusterItem>> entry : listOfEntry) {
                        ArrayList<ClusterItem> nList = entry.getValue();
                        ArrayList<ClusterItem> topK = MapReduceJob2Mapper.TOP_K_MAP.get(entry.getKey());

                        System.out.println("FINDING TOP-K FOR QUERY = \t" + entry.getKey());
                        System.out.println("nList size = \t" + nList.size());

                        MapReduceJob2Helper.findTopKIncrement(context, nList, topK);
                    }
                } //  Chi co multi-query
                else {
                    Set<Entry<String, ArrayList<ClusterItem>>> entrySet = hashmap.entrySet();
                    ArrayList<Entry<String, ArrayList<ClusterItem>>> listOfEntry = new ArrayList<Entry<String, ArrayList<ClusterItem>>>(entrySet);
                    for (Entry<String, ArrayList<ClusterItem>> entry : listOfEntry) {
                        ArrayList<ClusterItem> nList = entry.getValue();

                        MapReduceJob2Helper.findTopKQuery(context, nList);
                    }
                }
            } //  1 Query, increment
            else if (MapReduceJob2Mapper.IS_INCREMENT) {
                MapReduceJob2Helper.findTopKIncrement(context, list, MapReduceJob2Mapper.TOP_K);
            } // 1 query 
            else {
                MapReduceJob2Helper.findTopKQuery(context, list);
            }
        }
    }
    //***
    public static class CaderPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String[] s = key.toString().split(",");
            String url = s[0];
            String query = s[1];
            int numReduceTask = -1;
            for (String q : MapReduceJob2Mapper.QUERY_LIST) {
                if (query.equals(q)) {
                    return numReduceTask++;
                }
            }
            return 0;
        }
    }
    public static void main(String[] args) throws Exception {
        // MapReduce Job 2: Find Top-K
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job job = Job.getInstance(conf, "MapReduceJob2");
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.addCacheFile(new Path(args[2]).toUri());
        job.setJarByClass(MapReduceJob2.class);
        job.setMapperClass(MapReduceJob2Mapper.class);
//        job.setCombinerClass(MapReduceJob2Reduce.class);
        
        job.setPartitionerClass(CaderPartitioner.class);
        //4 la so cau query trong query.txtf  nếu đc thì đem việc đọc câu query ra ngoài ***
        //ham main vi nếu file input lớn, sẽ tạo ra nhìu mapper -> đoc nhìu lần setup -> đọc nhiu lần file query.txt,
        // cho phép tao nhìu num task, nếu dư thì bỏ qua.
        job.setNumReduceTasks(5);
        
        
        job.setReducerClass(MapReduceJob2Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        System.out.println("RUN TIME = " + (System.currentTimeMillis() - startTime));
    }

}
