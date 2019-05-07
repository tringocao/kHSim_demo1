
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.lang.Math;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author minhquang
 */
public class SpiralClusterBuilder {

    public static class SpiralClusterMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String doc = value.toString();

            //  Split to get username and tweet            
            String[] obj = doc.split(",", 2);

            Text Cluster = null;
            String kshingle = kHSimHelper.genShingle(obj[1]);
            String kshingles[] = kshingle.split("@");
            int shinglesNumber = Integer.parseInt(kshingles[0]);
            Text cluster = new Text(Double.toString(Math.floor(shinglesNumber / kHSimHelper.LAMDA + 1)));
            
            //writing the ByteArrayOutputStream bout 
            context.write(cluster, new Text(obj[0] + "@" + kshingle));
        }
    }

    public static class SpiralClusterReduce extends Reducer<Text, Text, Text, Text> {
        static enum CountersEnum {     
            NUM_OF_CLUSTER_CREATED
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder res = new StringBuilder();
            for (Text val : values) {
                res.append(val.toString()).append("#");
            }

            context.getCounter(CountersEnum.class.getName(), CountersEnum.NUM_OF_CLUSTER_CREATED.toString()).increment(1);

            context.write(key, new Text(res.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        // Map Reduce Job 1: Build Spiral Cluster structure
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "SpiralClusterBuilder");
        job1.setNumReduceTasks(10);
        job1.setJarByClass(SpiralClusterBuilder.class);
        job1.setMapperClass(SpiralClusterMapper.class);
        job1.setReducerClass(SpiralClusterReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
    }
}
