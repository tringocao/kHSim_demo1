
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.lang.Math;
import java.security.NoSuchAlgorithmException;
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

public class SpiralClusterBuilder {

    public static class SpiralClusterMapper extends Mapper<Object, Text, Text, Text> {

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
            String kshingle = null;
            try {
                //Open file
                dis = fileToRead.open(path);
                byte tempBuffer[] = new byte[1024];
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                while (dis.read(tempBuffer, 0, tempBuffer.length) >= 0) {
                    text = new String(tempBuffer);
                }

                //Generating shingle
                Shingle shingle = new Shingle();
                kshingle = shingle.genShingle(text);

                //writing the ByteArrayOutputStream bout 
                context.write(new Text(pathToRead), new Text(kshingle));
            } finally {
                dis.close();
                IOUtils.closeStream(dis);
            }
        }
    }

    public static class SpiralClusterReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text cluster = null;

            for (Text tempPath : values) {
                String kshingle[] = tempPath.toString().split(";");
                int shinglesNumber = kshingle.length;
                key = new Text(key.toString() + "@" + shinglesNumber + "@" + tempPath);
                cluster = new Text(new Double(Math.floor(shinglesNumber / 2.0 + 1)).toString());
            }
            context.write(cluster, key);
        }
    }

    public static void main(String[] args) throws Exception {
        //	Map Reduce Job 1: Build Spiral Cluster structure
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "SpiralClusterBuilder");

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
