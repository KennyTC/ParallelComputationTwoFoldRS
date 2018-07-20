import java.io.*;
import com.google.common.base.Joiner;
import java.util.StringTokenizer;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.ArrayWritable; 
import java.util.concurrent.TimeUnit;

/**
 * Aggregate certain and possible equivalence class in parallel.
 */
public class RA
{
    public static void main(String [] args) throws Exception
    {
        long startTime = System.nanoTime();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[2]);
        job.setJarByClass(RA.class);
        job.setMapperClass(MapRA.class);
        job.setCombinerClass(CombinerRA.class);
        job.setReducerClass(ReduceRA.class);       
        job.setNumReduceTasks(Integer.parseInt(args[3]));

        //input
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 2);

        //job output, compressed by Snappy 
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class,CompressionCodec.class);

        // job output, compressed by Snappy
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true); 
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class); 

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Boolean status = job.waitForCompletion(true);
        long stopTime = System.nanoTime();
        System.out.println("Time" + TimeUnit.NANOSECONDS.toSeconds(stopTime-startTime));
        System.exit( status ? 0 : 1);
    }

    public static List<List<Integer>> readSequenceFile(String uri)throws IOException{
        List<List<Integer>> temp = new ArrayList<List<Integer>>();
        Configuration conf = new Configuration();
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {       
            reader = new SequenceFile.Reader(conf, Reader.file(path), Reader.bufferSize(4096), Reader.start(0));
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                String[] v= value.toString().split("\t");
                String[] v1= v[1].split("\\|");
                for(String v2:v1){
                    StringTokenizer st = new StringTokenizer(v2, ",");                  
                    List<Integer> myList = new ArrayList<Integer>();
                    while(st.hasMoreTokens()){
                        myList.add(Integer.parseInt(st.nextToken()));
                    }
                    temp.add(myList); 
                }          
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        return temp;
    }

    // return True if arr1 contains arr2, False otherwise. Both arr1 and arr2 are sorted. 
    public static Boolean contain(List<Integer> arr1, List<Integer> arr2) {
        if (arr1.size() < arr2.size()) return false;
        for (int v: arr2){
            if (!arr1.contains(v)){
                return false;
            }
        }
        return true;            
    }
    // find intersection between two List<Integer>. Both arr1 and arr2 are sorted.  
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

    // convert a string to List<Integer>
    public static List<Integer> convertToList(String v) {

        List<Integer> myList = new ArrayList<Integer>();
        if (!v.contains(","))
            myList.add(Integer.parseInt(v));
        else {
            StringTokenizer st = new StringTokenizer(v, ",");       
            while(st.hasMoreTokens()){
                myList.add(Integer.parseInt(st.nextToken()));
            }
            Collections.sort(myList);
        }
        return myList;        
    }
    // check arr1 to see if it is arr2. Return true if arr1 is in arr2.
    public static Boolean exists(List<Integer> arr1, List<List<Integer>> arr2)
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

    public static class MapRA extends Mapper<LongWritable, Text, Text, Text>{
        public static List<List<Integer>> minA = new ArrayList<List<Integer>>();
        public static List<List<Integer>> maxA = new ArrayList<List<Integer>>();
        public Text CL = new Text();
        public Text CU = new Text();
        public Text PL = new Text();
        public Text PU = new Text();

        // load results of AP 
        protected void setup(Context context) throws IOException, InterruptedException {

            System.out.println("start setup");            
            minA=readSequenceFile("/output/100/8attr/app_min/part-r-00000"); // load Certian equivalence classes on A
            maxA=readSequenceFile("/output/100/8attr/app_max/part-r-00000"); // load possible equivalence classes on A           
            System.out.println("finish setup");
            if (minA.isEmpty())
                System.out.println("minA empty");
            if (maxA.isEmpty())
                System.out.println("maxA empty");

        }        

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            System.out.println("start map");            
            String[] v=value.toString().split("\t");
            System.out.println("d ="+v[0]);

            // initilize and sort x
            StringTokenizer st = new StringTokenizer(v[1], ",");
            List<Integer> x = new ArrayList<Integer>(v[1].trim().split(",").length);
            while(st.hasMoreTokens()){
                x.add(Integer.parseInt(st.nextToken()));
            }
            // Collections.sort(x);
            System.out.println("size of x: "+ x.size());
            CL.set("CL");
            CU.set("CU");
            PL.set("PL");
            PU.set("PU");

            Joiner comma = Joiner.on(",").skipNulls(); 
            Text tmp = new Text();
            // calculate certain lower approximation (CL) and certain upper approximation (CU)
            for(List<Integer> min: minA){
                for(List<Integer> max: maxA){
                    if(contain(min,max) && contain(x,max)){
                        tmp.set(comma.join(min));
                        con.write(CL,tmp);
                        System.out.println("CL:"+min.size());
                    }
                }             

                List<Integer> intersect = intersection(min,x);
                if (!intersect.isEmpty()){
                    tmp.set(comma.join(intersect));                   
                    con.write(CU,tmp);   
                    System.out.println("CU:"+intersect.size());
                }
            }

            // calculate possible lower approximation (PL) and possible upper approximation (PU)
            for(List<Integer> max: maxA){
                for(List<Integer> min: minA){
                    if(contain(max,min) && contain(x,min)){
                        List<Integer> intersect= intersection(max,x);
                        if (!intersect.isEmpty()){
                            tmp.set(comma.join(intersect));                       
                            con.write(PL,tmp);
                            System.out.println("PL:"+intersect.size());
                        }
                    }
                }
                List<Integer> intersect = intersection(max,x);
                if (!intersect.isEmpty()){
                    tmp.set(comma.join(max));                  
                    con.write(PU,tmp);     
                    System.out.println("PU:"+intersect.size());
                }
            }

            System.out.println("finish map");
        }
    }
    
    public static class ReduceRA extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
        {
            System.out.println("start reducer, key="+ key.toString());
            List<List<Integer>> tmp = new ArrayList<List<Integer>>();
            for(Text value: values){
                List<Integer> list= convertToList(value.toString());
                if (!exists(list,tmp)){
                    tmp.add(list);
                }
            }
            Text tx = new Text();
            Joiner comma = Joiner.on(",").skipNulls(); 
            System.out.println("tmp size: "+tmp.size());

            // sort by length of the list
            Collections.sort(tmp, new Comparator(){ 
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Comparable) ((List<Integer>) (o1)).size()).compareTo(((List<Integer>) (o2)).size());
                    }
                });
            for(List<Integer> l: tmp){
                tx.set(comma.join(l));
                con.write(key,tx);
                System.out.println("l size: "+l.size());
            }
            System.out.println("stop reducer");

        }
    }
    public static class CombinerRA extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
        {
            System.out.println("start combiner, key="+ key.toString());
            List<List<Integer>> tmp = new ArrayList<List<Integer>>();
            for(Text value: values){
                List<Integer> list= convertToList(value.toString());
                if (!exists(list,tmp)){
                    tmp.add(list);
                }
            }
            Text tx = new Text();
            Joiner comma = Joiner.on(",").skipNulls(); 
            System.out.println("tmp size: "+tmp.size());
            for(List<Integer> l: tmp){
                tx.set(comma.join(l));
                con.write(key,tx);
                System.out.println("l size: "+l.size());
            }
            System.out.println("finish combiner");
        }
    }
}
