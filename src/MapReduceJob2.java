
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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author minhquang
 */
public class MapReduceJob2 {

    public static class MapReduceJob2Mapper extends Mapper<Object, Text, Text, Text> {

        static enum CountersEnum {
            NUM_OF_CLUSTER_READ
        }

        private Configuration conf;
        private BufferedReader fis;
        private Set<String> queryRead = new HashSet<String>();

        private String QUERY = "";
        private double LB;
        private double UB;
        private String queryShingle;
        private int queryShingleLength;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            URI[] queryURIs = Job.getInstance(conf).getCacheFiles();
            for (URI queryURI : queryURIs) {
                Path queryPath = new Path(queryURI.getPath());
                String queryFileName = queryPath.getName();
                parseQuery(queryFileName);
            }

            for (String i : queryRead) {
                QUERY = i;
                break;
            }
            QUERY = QUERY.trim();

            //  Gen shingle query
            queryShingle = kHSimHelper.uniqueQuery(kHSimHelper.genShingle(QUERY).split("@")[1]);

            // Find cluster for query
            String kQueryShingle[] = queryShingle.split(";");
            queryShingleLength = kQueryShingle.length;

            LB = kHSimHelper.findLB(queryShingleLength, kHSimHelper.DEFAULT_EPSILON);
            UB = kHSimHelper.findUB(queryShingleLength, kHSimHelper.DEFAULT_EPSILON);
        }

        private void parseQuery(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String query = null;
                while ((query = fis.readLine()) != null) {
                    queryRead.add(query);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().toString();
            //  Get item list
            ArrayList<ClusterItem> itemList = kHSimHelper.readCluster(content);

            int clusterId = itemList.get(0).getClusterId();

            //  Check if clusterId suitable
            boolean isRightCluster = ((double) clusterId > LB) && ((double) clusterId < UB);
            boolean isEnoughItem = itemList.size() >= kHSimHelper.K;

            if (isRightCluster) {
                context.getCounter(CountersEnum.class.getName(), CountersEnum.NUM_OF_CLUSTER_READ.toString()).increment(1);

                ClusterItem[] chosenItems;
                boolean isSatisfy = false;
                int stepCount = itemList.size() + 1;
                ClusterItem largestItem = new ClusterItem();
                ClusterItem smallestItem = new ClusterItem();

                while (isSatisfy == false && stepCount >= 0) {
                    if (isEnoughItem) {
                        chosenItems = kHSimHelper.getKRandomItem(itemList, kHSimHelper.K);
                    } else {
                        chosenItems = itemList.toArray(new ClusterItem[itemList.size()]);
                    }

                    ArrayList<ClusterItem> calculatedItems = new ArrayList<>();
                    for (ClusterItem item : chosenItems) {
                        double similarityScore = kHSimHelper.calculateSim(queryShingle, item.getSh());
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

                // Find cluster for query with new max epsilon 
                double maxLB = kHSimHelper.findLB(queryShingleLength, epsilon1);
                double maxUB = kHSimHelper.findUB(queryShingleLength, epsilon1);

                boolean isRightClusterMax = ((double) clusterId > maxLB) && ((double) clusterId < maxUB);

                // Find cluster for query with new min epsilon 
                double minLB = kHSimHelper.findLB(queryShingleLength, epsilon2);
                double minUB = kHSimHelper.findUB(queryShingleLength, epsilon2);

                boolean isRightClusterMin = false;
                if (epsilon1 > epsilon2) {
                    isRightClusterMin = ((double) clusterId > minLB) && ((double) clusterId < minUB);
                }

                //  Calculate all sim
                ArrayList<ClusterItem> topKMax = new ArrayList<>();
                ArrayList<ClusterItem> topKMin = new ArrayList<>();

                if (isRightClusterMax) {
                    for (ClusterItem item : itemList) {
                        double similarityScore = kHSimHelper.calculateSim(queryShingle, item.getSh());
                        item.setSimilarityScore(similarityScore);
                        if (similarityScore >= epsilon1) {
                            topKMax.add(item);
                        } else if (similarityScore >= epsilon2) {
                            topKMin.add(item);
                        }
                    }
                } else if (isRightClusterMin) {
                    for (ClusterItem item : itemList) {
                        double similarityScore = kHSimHelper.calculateSim(queryShingle, item.getSh());
                        item.setSimilarityScore(similarityScore);
                        if (similarityScore >= epsilon2) {
                            topKMin.add(item);
                        }
                    }
                }

                if (topKMax.size() >= kHSimHelper.K) {
                    Collections.sort(topKMax);
                    int size = topKMax.size() - 1;
                    for (int i = 0; i < kHSimHelper.K; i++) {
                        context.write(new Text(topKMax.get(size - i).getUrl()),
                                new Text(String.valueOf(topKMax.get(size - i).getSimilarityScore())));
                    }
                } else {
                    int remain = kHSimHelper.K - topKMax.size();
                    int size = topKMin.size() - 1;
                    if (size >= 0) {
                        Collections.sort(topKMin);
                        int count = remain;
                        if (remain > (size + 1)) {
                            count = size + 1;
                        }
                        for (int i = 0; i < count; i++) {
                            topKMax.add(topKMin.get(size - i));
                        }

                        for (ClusterItem item : topKMax) {
                            context.write(new Text(item.getUrl()),
                                    new Text(String.valueOf(item.getSimilarityScore())));
                        }
                    }
                }
            }
        }

    }

    public static class MapReduceJob2Reduce extends Reducer<Text, Text, Text, Text> {
        ArrayList<ClusterItem> list = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double maxScore = 0.0D;
            for (Text similarityScore : values) {               
                double sim = Double.parseDouble(similarityScore.toString());                
                if (sim > maxScore) {
                    maxScore = sim;
                }
            }            
            list.add(new ClusterItem(key.toString(), maxScore));
//            context.write(key, new Text(String.valueOf(maxScore)));
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(list);
            int size = list.size() - 1;
            for(int i = 0; i < kHSimHelper.K; i++){
                ClusterItem item = list.get(size - i);
                context.write(new Text(item.getUrl()), new Text(String.valueOf(item.getSimilarityScore())));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Now go to MR2 with max epsilon
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduceJob2");
        job.addCacheFile(new Path(args[2]).toUri());
        job.setJarByClass(MapReduceJob2.class);
        job.setMapperClass(MapReduceJob2Mapper.class);
        job.setCombinerClass(MapReduceJob2Reduce.class);
        job.setReducerClass(MapReduceJob2Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }

}
