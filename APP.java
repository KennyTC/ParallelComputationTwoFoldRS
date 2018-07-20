import java.io.*;
import com.google.common.base.Joiner;
import java.util.StringTokenizer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
/**
 * Aggregate certain and possible equivalence classes in parallel
 */
public class APP extends Configured implements Tool 
{
    public static void main(String [] args) throws Exception
    {
        int exitCode = ToolRunner.run(new APP(), args);
        System.exit(exitCode);
        
    }

    public int run(String[] args) throws Exception {
        long startTime = System.nanoTime();
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[2]);
        job.setJarByClass(APP.class);
        job.setMapperClass(MapAPP.class);
        job.setPartitionerClass(PartitionerAPP.class);
        job.setReducerClass(ReduceAPP.class);
        job.setNumReduceTasks(Integer.parseInt(args[3]));

        // job input
        job.setInputFormatClass(SequenceFileInputFormat.class);       
        
        // map output, compressed using Snappy
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS,true);
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class,CompressionCodec.class);

        // job output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true); 
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);// just only effect when job output is SequenceFile
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class); 

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int status = job.waitForCompletion(true) ? 0:1;
        long stopTime = System.nanoTime();
        System.out.println("Time" + TimeUnit.NANOSECONDS.toSeconds(startTime - stopTime));
        return status;
    }
    // partitioner
    public static class PartitionerAPP extends Partitioner<Text,Text>{    

        public int getPartition(Text key, Text value, int numReduceTasks)
        {
            if (numReduceTasks==1) return 1;
            if (numReduceTasks==0) return 0;
            int ret=0;
            String[] v= value.toString().split("\t");
            if (numReduceTasks==2) {
                if (value.toString().startsWith("2")){
                    ret=0;
                }
                else ret=1;
            }
            if (numReduceTasks==3) {
                Random rand = new Random(); 
                ret = rand.nextInt(3);//return 0,1,2 
            }
            if (numReduceTasks==4){

                if (v[0].startsWith("1")){
                    ret=1;
                }
                else if (v[0].startsWith("2")){
                    ret=2;
                }
                else if (v[0].startsWith("3")){
                    ret=3;
                }
                else {
                    ret=0;
                }
            }
            System.out.println("key: "+ v[0] + "goes to " + ret + " partition"+", numReduceTasks="+numReduceTasks);
            return ret;
        }
    }
    public static class MapAPP extends Mapper<Text, Text, Text, Text>{     
        public static int k=0;

        public List<List<Integer>> c_pre;
        public List<List<Integer>> c_current;
        public List<List<Integer>> c_result;
        public List<String> strKey;

        public void setup(Context con) throws IOException, InterruptedException
        {
            c_pre = new ArrayList<>();
            c_current = new ArrayList<>();
            c_result = new ArrayList<>();
            strKey = new ArrayList<>();
        }

        public void map(Text key, Text value, Context con) throws IOException, InterruptedException
        {

            System.out.println("key: "+ key);
            //aggregate max               
            if (k!=100){
                if (k==0){
                    c_pre= convertToListList(value.toString());
                    strKey.add(key.toString());                   
                    k=1;
                }else{
                    c_current= convertToListList(value.toString()); 
                    for (List<Integer> p: c_pre){                   
                        for (List<Integer> c: c_current){                              
                            List<Integer> intersect=intersection(c,p);
                            if (!intersect.isEmpty()){
                                if(!exists(intersect,c_result))
                                    c_result.add(intersect);
                            }                                              
                        }                   
                    }
                    if (c_result.isEmpty()){
                        k=100;                  
                        System.out.println("key: "+key+" not_exist");
                    } else {                    
                        c_pre=new ArrayList<List<Integer>>(c_result);       
                        c_result.clear(); 
                        System.out.println("key: "+ key + " OK");
                        strKey.add(key.toString());
                    }
                }                   
            }

        }

        protected void cleanup(Context con) throws IOException, InterruptedException {
            Joiner pipe = Joiner.on("|").skipNulls(); 
            Joiner comma = Joiner.on(",").skipNulls(); 
            String sKey= comma.join(strKey);
            if (k==100){
                con.write(new Text("1"), new Text(sKey+"\t"+"not_exist"));
                System.out.println(sKey+"\t"+"not_exist");
            }else {

                List<String> tmp = new ArrayList<String>();
                for(List<Integer> st: c_pre){                    
                    String s= comma.join(st);
                    tmp.add(s);
                }
                String max_str = pipe.join(tmp);  
                if (!max_str.isEmpty()){                    
                    con.write(new Text("1"), new Text(sKey+"\t"+max_str));  
                    System.out.println(sKey+"\t"+"OK");           
                }
            }
            c_pre.clear();
            c_current.clear();
            c_result.clear();
            System.out.println("Map finished");
        }
        // convert a string to a List<List<Integer>>
        public static List<List<Integer>> convertToListList(String v) {

            String[] vStr= v.trim().split("\\|");       
            List<List<Integer>> tmp = new ArrayList<List<Integer>>();
            for (String s: vStr){            
                StringTokenizer st = new StringTokenizer(s, ",");
                int len = s.trim().split(",").length;
                List<Integer> myList = new ArrayList<Integer>();
                while(st.hasMoreTokens()){
                    myList.add(Integer.parseInt(st.nextToken()));
                }
                tmp.add(myList);
            }   
            return tmp;
        }
        // find intersection between two List<Integer>
        public static List<Integer> intersection(List<Integer> arr1, List<Integer> arr2)
        {
            List<Integer> tmp = new ArrayList<Integer>();
            int i = 0, j = 0;
            int m = arr1.size();
            int n= arr2.size();
            while (i < m && j < n)
            {
                if (arr1.get(i) < arr2.get(j))
                    i++;
                else if (arr2.get(j) < arr1.get(i))
                    j++;
                else
                {
                    tmp.add(arr2.get(j));
                    j++;
                    i++;
                }
            }
            return tmp;
        }
        // check arr1 to see if it is arr2. Return true if arr1 is in arr2.
        static Boolean exists(List<Integer> arr1, List<List<Integer>> arr2)
        {
            Boolean exist=false;
            for (List<Integer> l : arr2){
                if (arr1.equals(l)){
                    exist=true;
                    break;
                }                    
            }
            return exist;
        }
    } 
    public static class ReduceAPP extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
        {
            System.out.println("Start reducer");
            List<List<Integer>> c_pre = new ArrayList<>();
            List<List<Integer>> c_current = new ArrayList<>();
            List<List<Integer>> c_result = new ArrayList<>();
            List<String> strKey = new ArrayList<>();
            int k=0;

            // aggregate
            for(Text value: values){
                String[] v=value.toString().split("\t");
                String sKey=v[0];
                String sValue=v[1];
                System.out.println("key: "+ sKey);
                if (sValue.equals("not_exist")){
                    k=100;
                    strKey.add(sKey);
                    break;
                }
                if (k==0){
                    c_pre= convertToListList(sValue);
                    strKey.add(sKey);
                    k=1;
                }else{
                    c_current= convertToListList(sValue); 
                    for (List<Integer> p: c_pre){                   
                        for (List<Integer> c: c_current){                              
                            List<Integer> intersect=intersection(c,p);
                            if (!intersect.isEmpty()){
                                if(!exists(intersect,c_result)){
                                    c_result.add(intersect);
                                }
                            }                                              
                        }                   
                    }
                    if (c_result.isEmpty()){
                        k=100;  
                        System.out.println(sKey+"\t"+"not_exist");
                        break;
                    } else {                    
                        c_pre=new ArrayList<List<Integer>>(c_result);       
                        c_result.clear();    
                        System.out.println(sKey +"\t"+"OK");
                        strKey.add(sKey);
                    }
                }                  
            }
            Joiner pipe = Joiner.on("|").skipNulls(); 
            Joiner comma = Joiner.on(",").skipNulls(); 
            String sKey=comma.join(strKey);
            if (k==100){
                con.write(new Text("1"), new Text(sKey +"\t"+"not_exist"));
            }else {
                List<String> tmp = new ArrayList<String>();
                for(List<Integer> st: c_pre){                    
                    String s= comma.join(st);
                    tmp.add(s);
                }
                String max_str = pipe.join(tmp);  
                if (!max_str.isEmpty()){
                    con.write(new Text("1"), new Text(sKey+"\t"+max_str));  
                    System.out.println(sKey+"\t"+"OK");  
                    System.out.println("Reducer: finished");          
                }
            }
        }
        
        static Boolean exists(List<Integer> arr1, List<List<Integer>> arr2)
        {
            Boolean exist=false;
            for (List<Integer> l : arr2){
                if (arr1.equals(l)){
                    exist=true;
                    break;
                }                    
            }
            return exist;
        }

        public static List<List<Integer>> convertToListList(String v) {

            String[] vStr= v.trim().split("\\|");       
            List<List<Integer>> tmp = new ArrayList<List<Integer>>();
            for (String s: vStr){            
                StringTokenizer st = new StringTokenizer(s, ",");
                int len = s.trim().split(",").length;
                List<Integer> myList = new ArrayList<Integer>();
                while(st.hasMoreTokens()){
                    myList.add(Integer.parseInt(st.nextToken()));
                }
                tmp.add(myList);
            }   
            return tmp;
        }

        public static List<Integer> intersection(List<Integer> arr1, List<Integer> arr2)
        {
            List<Integer> tmp = new ArrayList<Integer>();
            int i = 0, j = 0;
            int m = arr1.size();
            int n= arr2.size();
            while (i < m && j < n)
            {
                if (arr1.get(i) < arr2.get(j))
                    i++;
                else if (arr2.get(j) < arr1.get(i))
                    j++;
                else
                {
                    tmp.add(arr2.get(j));
                    j++;
                    i++;
                }
            }
            return tmp;
        }

    }
}   

