
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.lang.Math;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
//import midprocess.ClusterItem;
//import midprocess.Shingle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;

public class MapReduceJob2 {

    public static class MapReduceJob2Mapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //Reading path from the value passed above in map where the file is present.		
            String pathToRead = value.toString();

            Configuration conf = context.getConfiguration();
            Path path = new Path(pathToRead);
            //Creating a FileSystem setup from the above path		

            FileSystem fileToRead = FileSystem.get(URI.create(pathToRead), conf);

            //creating a DataInput stream class where it reads the file and outputs bytes stream
            DataInputStream dis = null;

            String text = null;
            ArrayList<String> resList = new ArrayList<>();
            String kshingle = null;

            try {
                //Open file
                dis = fileToRead.open(path);
                byte tempBuffer[] = new byte[1024];
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                while (dis.read(tempBuffer, 0, tempBuffer.length) >= 0) {
                    resList.add(new String(tempBuffer));
                }
                
                //
                //  Logic error in reading file
                //

                //  First 5 line is query, epsilon and cluster info
                String queryShingle = resList.get(0);
                int epsilon1 = Integer.parseInt(resList.get(1));
                int epsilon2 = Integer.parseInt(resList.get(3));
                int clusterId = Integer.parseInt(resList.get(2));      //  currently there's only one cluster found

                //  Build cluster structure based on remaining line
                HashMap<Integer, ArrayList<ClusterItem>> hashmap = new HashMap<>();
                for (int i = 5; i < resList.size(); i++) {
                    ClusterItem item = convertToClusterItem(resList.get(i));
                    ArrayList<ClusterItem> list = new ArrayList<>();
                    if (hashmap.get(item.getClusterId()) != null) {
                        list = hashmap.get(item.getClusterId());
                    }
                    list.add(item);
                    hashmap.put(item.getClusterId(), list);
                }

                //  Get cluster based on clusterId and calculate similarity score
                ArrayList<ClusterItem> cluster = hashmap.get(clusterId);
                ArrayList<ClusterItem> similarityItems = new ArrayList<>();
                for (ClusterItem item : cluster) {
                    int similarityScore = new Shingle().compare(item.getSh(), queryShingle);
                    if (similarityScore < epsilon1) {
                        continue;
                    }
                    ClusterItem simItem = item;
                    simItem.setSimilarityScore(similarityScore);
                    similarityItems.add(simItem);
                }

                //writing the ByteArrayOutputStream bout 
                ClusterItem item = similarityItems.get(0);
                
                //
                //  ERROR HERE
                //
                
                context.write(new Text(String.valueOf(item.getUrl())),
                        new Text(String.valueOf(item.getSimilarityScore())));

            } finally {
                dis.close();
                IOUtils.closeStream(dis);
            }
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

    }

    public static class MapReduceJob2Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text val = new Text("");
            for (Text temp : values) {
                val = new Text(val.toString() + "@" + temp.toString());
            }
            context.write(key, val);
        }
    }

    public static void main(String[] args) throws Exception {
        //	Map Reduce Job 2: Get similarity score and show result
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduceJob2");

        job.setJarByClass(MapReduceJob2.class);
        job.setMapperClass(MapReduceJob2Mapper.class);
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
