import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.compress.CompressionCodec;
/**
 * Calculate equivalence classes in parallel
 *
 */
public class EC
{
    public static void main(String [] args) throws Exception
    {
        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[2]);
        job.setJarByClass(EC.class);
        job.setMapperClass(MapEC.class);
        job.setCombinerClass(ReduceEC.class);
        job.setReducerClass(ReduceEC.class);
        job.setNumReduceTasks(Integer.parseInt(args[3]));

        // job input
        job.setInputFormatClass(SequenceFileInputFormat.class); 

        //map output, compressed using Snappy
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class,CompressionCodec.class);

        //job output,compressed using Snappy, set as SequenceFile
        job.setOutputFormatClass(SequenceFileOutputFormat.class);    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setCompressOutput(job, true); 
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Boolean status = job.waitForCompletion(true);
        long stopTime = System.nanoTime();
        System.out.println("Time" + TimeUnit.NANOSECONDS.toSeconds(startTime - stopTime));
        System.exit( status ? 0 : 1);


    }
    public static class MapEC extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            //System.out.println("Start map");
            String[] attributes = value.toString().trim().split(",");           
            for(int j=0; j< attributes.length;j++)
            {
                Text outputKey = new Text(j+1+","+attributes[j]);
                Text outputValue= new Text(String.valueOf(key));
                con.write(outputKey,outputValue);
            }
            // if (key.get()%1000==0)
            // System.out.println("key:"+key);

        }
    }
    public static class ReduceEC extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
        {
            List<String> tmp = new ArrayList<String>();
            for(Text value : values)
            {
                tmp.add(value.toString());
            }
            Joiner comma = Joiner.on(",").skipNulls(); 
            con.write(key, new Text(comma.join(tmp)));
        }
    }
}
