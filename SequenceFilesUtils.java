import java.io.File;
import java.io.IOException;
import java.lang.*;
import java.util.*;
import java.util.Collections;
import java.util.StringTokenizer;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*; 
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.commons.io.FileUtils;

/**
 * Utils class to handle SequenceFiles
 */
public class SequenceFilesUtils {

    public static void main(String[] args) throws IOException {

        String select = args[2];
        switch(select){
            case "long":
            longwritable(new Path(args[0]), new Path(args[1]),Integer.parseInt(args[3])); //convert a SequenceFile with Key=LongWritable to a new SequenceFile with different blocksizek       
            break;
            case "text":
            textwritable(new Path(args[0]),new Path(args[1]),Integer.parseInt(args[3])); //convert a SequenceFile with Key=Text to a new SequenceFile with different blocksize  
            break;
            case "convertattributetosequence":
            convertAttributesToSequenceFile(args[0],new Path(args[1]),Integer.parseInt(args[3])); //convert a text file to SequenceFile  
            break;
            case "removedupAP":
            removeDuplicateAPPResult(new Path(args[0]),new Path(args[1]));//remove duplicate result in APP step  
            break;
            case "selectattribute":
            selectAttributesToAggregate(new Path(args[0]),new Path(args[1]),Integer.parseInt(args[3]),args[4]); //extract some attributes from a SequenceFile, create a new SequenceFile and upload to hdfs  
            break;
        }
    }
    // convert a SequenceFile (Key LongWritable, value Text) to different blocksize SequenceFile 
    public static void longwritable(Path readPath, Path writePath, int size) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = null;
        writer = SequenceFile.createWriter(conf, Writer.file(writePath), 
            Writer.keyClass(LongWritable.class),
            Writer.valueClass(Text.class), 
            Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
            Writer.replication(fs.getDefaultReplication()),
            Writer.blockSize(size*1024*1024),
            Writer.compression(SequenceFile.CompressionType.BLOCK, new SnappyCodec()),
            Writer.progressable(null),
            Writer.metadata(new Metadata()));
        try {       
            for (FileStatus status : fs.listStatus(readPath)) {
                Path fPath = status.getPath();
                System.out.println("Merging '" + fPath.getName() + "'");
                if (fPath.getName().startsWith("_")) {
                    System.out.println("Skip \"_\"-file '" + fPath.getName() + "'"); //There are files such "_SUCCESS"-named in jobs' ouput folders 
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(fPath), Reader.bufferSize(4096), Reader.start(0));
                LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

                while (reader.next(key, value)) {
                    writer.append(key, value);
                }
                reader.close();
            }
        } finally {

            writer.close();
        }   
    }

    // convert a SequenceFile (Key Text, value Text) to different blocksize SequenceFile 
    public static void textwritable(Path readPath, Path writePath, int size) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = null;
        writer = SequenceFile.createWriter(conf, Writer.file(writePath), 
            Writer.keyClass(Text.class),
            Writer.valueClass(Text.class), 
            Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
            Writer.replication(fs.getDefaultReplication()),
            Writer.blockSize(size*1024*1024),
            Writer.compression(SequenceFile.CompressionType.BLOCK, new SnappyCodec()),
            Writer.progressable(null),
            Writer.metadata(new Metadata()));
        try {       
            for (FileStatus status : fs.listStatus(readPath)) {
                Path fPath = status.getPath();
                System.out.println("Merging '" + fPath.getName() + "'");
                if (fPath.getName().startsWith("_")) {
                    System.out.println("Skip \"_\"-file '" + fPath.getName() + "'"); //There are files such "_SUCCESS"-named in jobs' ouput folders 
                    continue;
                }

                SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(fPath), Reader.bufferSize(4096), Reader.start(0));
                Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

                while (reader.next(key, value)) {
                    writer.append(key, value);
                }
                reader.close();
            }
        } finally {

            writer.close();
        }   
    }
    // convert file _attributes.txt to SequenceFile so that it can input to EC. We also add number for each line
    public static void convertAttributesToSequenceFile(String readPath, Path writePath, int size) throws IOException{      
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);        
        LongWritable key = new LongWritable();
        Text value = new Text();
        File infile = new File(readPath);
        SequenceFile.Writer writer = null;
        try 
        {
            writer = SequenceFile.createWriter(conf, Writer.file(writePath), 
                Writer.keyClass(key.getClass()),
                Writer.valueClass(value.getClass()), 
                Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
                Writer.replication(fs.getDefaultReplication()),
                Writer.blockSize(size*1024*1024),
                Writer.compression(SequenceFile.CompressionType.BLOCK, new SnappyCodec()),
                Writer.progressable(null),
                Writer.metadata(new Metadata()));           
            long ctr = 1;
            for (String line : FileUtils.readLines(infile))
            {
                key.set(ctr++);
                value.set(line);                        
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }       

    }
    // output attributes to aggregates, used before AP Step. attr is attributes, divided by comma, eg 1,10,11,25,26,27
    public static void selectAttributesToAggregate(Path readPath, Path writePath, int size, String attr) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = null;
        List<Integer> tmp = new ArrayList<Integer>();
        StringTokenizer st = new StringTokenizer(attr, ",");       
        while(st.hasMoreTokens()){
            tmp.add(Integer.parseInt(st.nextToken()));
        }

        writer = SequenceFile.createWriter(conf, Writer.file(writePath), 
            Writer.keyClass(Text.class),
            Writer.valueClass(Text.class), 
            Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
            Writer.replication(fs.getDefaultReplication()),
            Writer.blockSize(size*1024*1024),
            Writer.compression(SequenceFile.CompressionType.BLOCK, new SnappyCodec()),
            Writer.progressable(null),
            Writer.metadata(new Metadata()));
        try {       
            for (FileStatus status : fs.listStatus(readPath)) {
                Path fPath = status.getPath();
                System.out.println("Merging '" + fPath.getName() + "'");
                if (fPath.getName().startsWith("_")) {
                    System.out.println("Skip \"_\"-file '" + fPath.getName() + "'"); //There are files such "_SUCCESS"-named in jobs' ouput folders 
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(fPath), Reader.bufferSize(4096), Reader.start(0));
                Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

                while (reader.next(key, value)) {
                    if (tmp.contains(Integer.parseInt(key.toString()))){
                        writer.append(key, value);
                        int len=value.toString().split("\\|").length;
                        System.out.println("key:" + key + ", size: "+ len + ", 0 - " + (len-1)); 
                    }
                }
                reader.close();
            }
        } finally {

            writer.close();
        }   
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
    // check if arr1 is in arr2. 
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
    // tp examine the result of RA. 
    public static void check_ras_result() throws IOException{
        Joiner comma = Joiner.on(",").skipNulls(); 
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);      
        List<List<Integer>> tmp = new ArrayList<List<Integer>>();

        for (FileStatus status : fs.listStatus(new Path("/media/hduser/ADATA/KDD/experiment/5node/50/part-r-00000"))) {
            Path fPath = status.getPath();           
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(fPath), Reader.bufferSize(4096), Reader.start(0));
            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                if (!value.toString().isEmpty()){
                    System.out.println(value);
                }
            }
            reader.close();
        }
    }
    // remove duplicate in the result of APP. Output a SequenceFile (key=length of a list, value = the list itself) 
    public static void removeDuplicateAPPResult(Path readPath, Path writePath) throws IOException{
        Joiner comma = Joiner.on(",").skipNulls(); 
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = null;
        writer = SequenceFile.createWriter(conf, Writer.file(writePath), 
            Writer.keyClass(LongWritable.class),
            Writer.valueClass(Text.class), 
            Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
            Writer.replication(fs.getDefaultReplication()),
            Writer.compression(SequenceFile.CompressionType.BLOCK, new SnappyCodec()),
            Writer.progressable(null),
            Writer.metadata(new Metadata()));
        List<List<Integer>> tmp = new ArrayList<List<Integer>>();
        System.out.println("Start.............................");
        try {       

            for (FileStatus status : fs.listStatus(readPath)) {
                Path fPath = status.getPath();
                System.out.println("Merging '" + fPath.getName() + "'");
                if (fPath.getName().startsWith("_")) {
                    System.out.println("Skip \"_\"-file '" + fPath.getName() + "'"); //There are files such "_SUCCESS"-named in jobs' ouput folders 
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(fPath), Reader.bufferSize(4096), Reader.start(0));
                Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

                while (reader.next(key, value)) {
                    if (!value.toString().isEmpty()){
                        String[] v = value.toString().split("\t");
                        System.out.println("key:"+ v[0]);
                        String[] v1 = v[1].trim().split("\\|");
                        int i=1;
                        for (String v2: v1){
                            List<Integer> list = convertToList(v2);  
                            if (!list.isEmpty()){
                                if (!exists(list,tmp)){
                                    tmp.add(list); 
                                    System.out.println((i++)+"-->add");
                                }
                            }
                        }

                    }
                }
                System.out.println("tmp length:"+ tmp.size());
                reader.close();
            }
            // sort by length of the list
            Collections.sort(tmp, new Comparator(){ 

                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Comparable) ((List<Integer>) (o1)).size()).compareTo(((List<Integer>) (o2)).size());
                    }

                });

            for (List<Integer> list: tmp){  
                String s= comma.join(list);
                writer.append(new LongWritable(list.size()),new Text(s));
            }            

        } finally {
            writer.close();
        }   
    }

}