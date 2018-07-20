import java.io.*;
import com.google.common.base.Joiner;
import java.util.StringTokenizer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * Calculate certain and possible equivalence classes of an attributes in parallel
 *
 */
public class PP
{
    public static void main(String [] args) throws Exception
    {
        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[2]);
        job.setJarByClass(PP.class);
        job.setMapperClass(MapPP.class);
        job.setReducerClass(ReducePP.class);
        job.setNumReduceTasks(Integer.parseInt(args[3]));

        // job input
        job.setInputFormatClass(SequenceFileInputFormat.class); 

        //map output, compressed using Snappy
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class,CompressionCodec.class);

        //job output, compressed using Snappy, set as SequenceFile
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setCompressOutput(job, true); 
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);        

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Boolean status = job.waitForCompletion(true);
        long stopTime = System.nanoTime();
        System.out.println("Time" + TimeUnit.NANOSECONDS.toSeconds(startTime - stopTime));
        System.exit( status ? 0 : 1);
    }
    public static class MapPP extends Mapper<Text, Text, Text, Text>{

        public void map(Text key, Text value, Context con) throws IOException, InterruptedException
        {
            String[] keys=key.toString().split(",");
            Text outputKey = new Text(keys[0]);
            Text outputValue = new Text(keys[1]+"|"+value);
            con.write(outputKey, outputValue);
        }
    }
    public static class ReducePP extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
        {
            Set<Integer> c_missing = new HashSet<>();
            List<Set<Integer>> c_regular = new ArrayList<>();
            List<String> c_cer = new ArrayList<>();
            List<String> c_poss = new ArrayList<>();

            // save data to c_missing, and c_regular
            for(Text value : values)
            {
                String[] v=value.toString().trim().split("\\|");
                if (v[0].contains("*")) {
                    c_missing=convertToIntegerSet(v[1]);
                }
                else{                   
                    c_regular.add(convertToIntegerSet(v[1]));
                }         
            }
            //create c_cer and c_poss 
            Joiner comma = Joiner.on(",").skipNulls(); 
            SortedSet sortedMiss = new TreeSet<Integer>(c_missing);
            for(Set<Integer> set: c_regular){
                SortedSet sortedReg = new TreeSet<Integer>(set);
                c_cer.add(comma.join(sortedReg));
                SortedSet sortedUnion = new TreeSet<Integer>(union(sortedReg,sortedMiss));
                c_poss.add(comma.join(sortedUnion));
            }
            c_poss.add(comma.join(sortedMiss));

            // write to output
            Joiner pipe = Joiner.on("|").skipNulls(); 
            String s_cer = pipe.join(c_cer);
            String s_poss = pipe.join(c_poss);
            con.write(key,new Text(s_cer+"\t"+s_poss));
        }

        public static <T> Set<T> union(Set<T> setA, Set<T> setB) {
            Set<T> tmp = new TreeSet<T>(setA);
            tmp.addAll(setB);
            return tmp;
        }
        // convert string to a set of integer
        public static HashSet<Integer> convertToIntegerSet(String v) {

            HashSet<Integer> tmp = new HashSet<Integer>();
            // v[1] is 1,2,3,4,5
            if (v.contains(",")){
                String[] vStr= v.trim().split(",");  
                for (String s: vStr){            
                    tmp.add(Integer.parseInt(s));                                
                }  
            }else { //v[1] la one number
                tmp.add(Integer.parseInt(v));
            }             
            return tmp;
        }
    }
}
