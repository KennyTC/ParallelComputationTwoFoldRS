import java.util.*;
import com.google.common.base.Joiner;
import java.io.*;
import java.lang.Object;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * The class implements TwoFold Rough Approximations by Sequential Algorithm
 *
 * @author (your name)
 * @version (a version number or a date)
 */
public class SequentialAlgorithm
{
    public static void main(String [] args) throws Exception{

        String select=args[0];
        switch(select){
            case "create": 
            createCertainAndPossibleEC(args[1]);
            break;
            case "selectattribute":
            selectAttributesToAggregate(args[1],args[2]);
            break;
            case "aggregate":    
            aggregateAttr(args[1]);
            break;            
            case "rough":
            roughApproximation(args[1],args[2],args[3]);
            break;
            default:
            break;
        }
    }
    // create certain and possible equivalence classes
    public static void createCertainAndPossibleEC(String fileName){               

        long startTime = System.nanoTime();
        String ec = fileName.replaceFirst(".txt","_ec.txt"); 
        String pec = fileName.replaceFirst(".txt","_pec.txt"); 
        Map<Integer,Map<String,Set<Integer>>> map = new HashMap<>();       
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            BufferedWriter bufferedECWriter = new BufferedWriter(new FileWriter(ec));
            BufferedWriter bufferedPECWriter = new BufferedWriter(new FileWriter(pec));
            String line = null;
            int j=1;            
            while((line = bufferedReader.readLine()) != null) {                         
                String[] v=line.toString().split(",");         
                for (int i=0;i<v.length;i++){
                    if (map.containsKey(i)){
                        Map<String,Set<Integer>> m = map.get(i);
                        if (m.containsKey(v[i])){
                            Set<Integer> s = m.get(v[i]);
                            s.add(j);
                        }
                        else{
                            Set<Integer> s = new TreeSet<Integer>();
                            s.add(j);
                            m.put(v[i],s);
                        }
                    }
                    else {
                        Map<String,Set<Integer>> m = new HashMap<>();
                        Set<Integer> s = new TreeSet<Integer>();
                        s.add(j);
                        m.put(v[i],s);
                        map.put(i, m);
                    }
                }
                j=j+1;               
            }

            Joiner comma = Joiner.on(",").skipNulls(); 
            Joiner pipe = Joiner.on("|").skipNulls(); 
            for(Map.Entry<Integer, Map<String,Set<Integer>>> m: map.entrySet()){   
                // System.out.println(m.getKey());
                Map<String,Set<Integer>> m2 = m.getValue();
                Set<Integer> c_missing = new TreeSet<>();
                if (m2.containsKey("*"))
                {
                    c_missing = m2.get("*");
                }
                List<String> tmpCer = new ArrayList<String>();
                List<String> tmpPoss = new ArrayList<String>();
                tmpPoss.add(comma.join(c_missing));
                for(Map.Entry<String, Set<Integer>> item: m2.entrySet()){  
                    if(!item.getKey().contains("*")){                           
                        Set<Integer> s= item.getValue();
                        tmpCer.add(comma.join(s));     
                        tmpPoss.add(comma.join(union(s,c_missing)));                        
                    }                   
                }

                bufferedECWriter.write((m.getKey()+1)+"\t"+pipe.join(tmpCer));
                bufferedECWriter.newLine();
                bufferedPECWriter.write((m.getKey()+1)+"\t"+pipe.join(tmpPoss));
                bufferedPECWriter.newLine();
                // System.out.println("cer:"+ (m.getKey()+1)+"\t"+pipe.join(tmpCer));
                // System.out.println("poss:" + (m.getKey()+1)+"\t"+pipe.join(tmpPoss));

            }
            // Always close files.
            bufferedReader.close();
            bufferedECWriter.close();
            bufferedPECWriter.close();

            long stopTime = System.nanoTime();
            System.out.println("Time" + TimeUnit.NANOSECONDS.toSeconds(startTime - stopTime));
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");                  
            // Or we could just do this: 
            // ex.printStackTrace();
        }        
    }

    public static <T> Set<T> union(Set<T> setA, Set<T> setB) {
        Set<T> tmp = new TreeSet<T>(setA);
        tmp.addAll(setB);
        return tmp;
    }

    public static void selectAttributesToAggregate(String fileName,String attr){

        String fileResult = fileName.replaceFirst(".txt","_attrToAgg.txt"); 
        List<Integer> tmp = new ArrayList<Integer>();
        StringTokenizer st = new StringTokenizer(attr, ",");       
        while(st.hasMoreTokens()){
            tmp.add(Integer.parseInt(st.nextToken()));
        }        
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileResult));
            String line = null;
            int j=1;            
            while((line = bufferedReader.readLine()) != null) {                         
                String[] v=line.toString().split("\t");
                if (tmp.contains(Integer.parseInt(v[0]))){
                    bufferedWriter.write(line);
                    bufferedWriter.newLine();
                }
            }

            // Always close files.
            bufferedReader.close();
            bufferedWriter.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");                  
            // Or we could just do this: 
            // ex.printStackTrace();
        }        
    }

    public static void aggregateAttr(String fileName){

        long startTime = System.nanoTime();
        //load data
        //List<List<Integer>> l1= new ArrayList<>(); List<List<Integer>> l5= new ArrayList<>(); List<List<Integer>> l7= new ArrayList<>();List<List<Integer>> l10= new ArrayList<>();
        List<List<Integer>> l12= new ArrayList<>(); List<List<Integer>> l13= new ArrayList<>(); List<List<Integer>> l17= new ArrayList<>();List<List<Integer>> l18= new ArrayList<>();
        List<List<Integer>> l25= new ArrayList<>(); List<List<Integer>> l27= new ArrayList<>(); List<List<Integer>> l34= new ArrayList<>();List<List<Integer>> l37= new ArrayList<>();
        List<List<Integer>> lResult= new ArrayList<>();
        List<List<Integer>> lTemp= new ArrayList<>();

        String fileResult = fileName.replaceFirst(".txt","_aggregated.txt"); 
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileResult));
            String line = null;
            int j=1;            
            while((line = bufferedReader.readLine()) != null) {                         
                String[] v=line.toString().split("\t");
                // if (Integer.parseInt(v[0])==1) l1= convertToListList(v[1]);
                // if (Integer.parseInt(v[0])==5) l5= convertToListList(v[1]);
                // if (Integer.parseInt(v[0])==7) l7= convertToListList(v[1]);
                // if (Integer.parseInt(v[0])==10) l10= convertToListList(v[1]);
                if (Integer.parseInt(v[0])==12) l12= convertToListList(v[1]);
                if (Integer.parseInt(v[0])==13) l13= convertToListList(v[1]);
                if (Integer.parseInt(v[0])==17) l17= convertToListList(v[1]);
                if (Integer.parseInt(v[0])==18) l18= convertToListList(v[1]);
                if (Integer.parseInt(v[0])==25) l25= convertToListList(v[1]);
                if (Integer.parseInt(v[0])==27) l27= convertToListList(v[1]);
                if (Integer.parseInt(v[0])==34) l34= convertToListList(v[1]);
                if (Integer.parseInt(v[0])==37) l37= convertToListList(v[1]);
            }
            // System.out.println("l1 size:"+ l1.size());System.out.println("l5 size:"+ l5.size());System.out.println("l7 size:"+ l7.size()); System.out.println("l10 size:"+ l10.size());
            System.out.println("l12 size:"+ l12.size());
            System.out.println("l13 size:"+ l13.size());System.out.println("l17 size:"+ l17.size());System.out.println("l18 size:"+ l18.size());
            System.out.println("l25 size:"+ l25.size());System.out.println("l27 size:"+ l27.size());
            System.out.println("l34 size:"+ l34.size());System.out.println("l37 size:"+ l37.size());
            System.out.println("lResult size:"+ lResult.size());
            Joiner comma = Joiner.on(",").skipNulls(); 
            // for(List<Integer> s1: l1){
            // for(List<Integer> s10: l10){
            for(List<Integer> s12: l12){
                for(List<Integer> s13: l13){
                    for(List<Integer> s25: l25){
                        for(List<Integer> s34: l34){
                            // for(List<Integer> s5: l5){
                            // for(List<Integer> s7: l7){
                            for(List<Integer> s17: l17){
                                for(List<Integer> s18: l18){
                                    for(List<Integer> s27: l27){
                                        for(List<Integer> s37: l37){
                                            // lTemp.add(s1);lTemp.add(s5);lTemp.add(s7);lTemp.add(s10);
                                            lTemp.add(s12);lTemp.add(s13);lTemp.add(s17);lTemp.add(s18);lTemp.add(s25);lTemp.add(s27);lTemp.add(s34);lTemp.add(s37);
                                            List<Integer> intersect=intersection(lTemp);
                                            lTemp.clear();
                                            if (!intersect.isEmpty()){
                                                if (!exists(intersect,lResult)){
                                                    lResult.add(intersect);  
                                                    System.out.println("lResult size:"+ lResult.size());
                                                }
                                            }
                                        }
                                    }
                                }
                                // }
                                // }
                            }
                        }
                    }
                }
            }
            // }
            // }
            System.out.println("Loop over lResult");
            // sort by length of the list
            Collections.sort(lResult, new Comparator(){ 

                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Comparable) ((List<Integer>) (o1)).size()).compareTo(((List<Integer>) (o2)).size());
                    }

                });

            for(List<Integer> l: lResult){
                String s = comma.join(l);
                bufferedWriter.write(s);
                bufferedWriter.newLine();
            }
            // Always close files.
            bufferedReader.close();
            bufferedWriter.close();
            long stopTime = System.nanoTime();
            System.out.println("Time" + TimeUnit.NANOSECONDS.toSeconds(startTime - stopTime));
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");                  
            // Or we could just do this: 
            // ex.printStackTrace();
        }        
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

    public static List<Integer> intersection(List<Integer> arr1, List<Integer> arr10,List<Integer> arr12,List<Integer> arr13,List<Integer> arr25,List<Integer> arr34){
        // Set<Integer> tmp1 = new HashSet<Integer>(arr1);
        // tmp1.retainAll(new HashSet<Integer>(arr10));
        // tmp1.retainAll(new HashSet<Integer>(arr12));
        // tmp1.retainAll(new HashSet<Integer>(arr13));
        // tmp1.retainAll(new HashSet<Integer>(arr25));
        // tmp1.retainAll(new HashSet<Integer>(arr34));
        List<Integer> tmp = new ArrayList<Integer>();
        tmp = pairIntersection(arr1, arr10);
        tmp = pairIntersection(tmp,arr12);
        tmp = pairIntersection(tmp,arr13);
        tmp = pairIntersection(tmp,arr25);
        tmp = pairIntersection(tmp,arr34);
        return tmp;        
    }

    public static List<Integer> intersection(List<List<Integer>> arr){

        List<Integer> tmp = pairIntersection(arr.get(0),arr.get(1));        
        for(int i=2; i<arr.size();i++)
        {
            tmp=pairIntersection(tmp,arr.get(i));
        }        
        return tmp;        
    }

    public static List<Integer> pairIntersection(List<Integer> arr1, List<Integer> arr2)
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

    public static void roughApproximation(String ecFileName, String pecFileName, String fileName) throws IOException {
        long startTime = System.nanoTime();
        //load data
        List<List<Integer>> minA= readAggregatedFile(ecFileName);
        List<List<Integer>> maxA= readAggregatedFile(pecFileName);
        System.out.println("size of minA: "+ minA.size());
        System.out.println("size of maxA: "+ maxA.size());
        String fileCL = fileName.replaceFirst(".txt","_roughCL.txt"); 
        String fileCU = fileName.replaceFirst(".txt","_roughCU.txt"); 
        String filePL = fileName.replaceFirst(".txt","_roughPL.txt"); 
        String filePU = fileName.replaceFirst(".txt","_roughPU.txt");
        List<List<Integer>> CLResult= new ArrayList<>();
        List<List<Integer>> CUResult= new ArrayList<>();
        List<List<Integer>> PLResult= new ArrayList<>();
        List<List<Integer>> PUResult= new ArrayList<>();
        Joiner comma = Joiner.on(",").skipNulls(); 
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            BufferedWriter bufferedCLWriter = new BufferedWriter(new FileWriter(fileCL));
            BufferedWriter bufferedCUWriter = new BufferedWriter(new FileWriter(fileCU));
            BufferedWriter bufferedPLWriter = new BufferedWriter(new FileWriter(filePL));
            BufferedWriter bufferedPUWriter = new BufferedWriter(new FileWriter(filePU));
            String line = null;
            int j=1;            
            while((line = bufferedReader.readLine()) != null) {                         
                String[] v=line.toString().split("\t");
                // initilize and sort x
                StringTokenizer st = new StringTokenizer(v[1], ",");
                List<Integer> x = new ArrayList<Integer>(v[1].trim().split(",").length);
                while(st.hasMoreTokens()){
                    x.add(Integer.parseInt(st.nextToken()));
                }
                Collections.sort(x);
                System.out.println("size x: "+ x.size());
                System.out.println("start " + v[0]);
                // calculate certain lower approximation (CL) and certain upper approximation (CU)
                for(int i=0;i<minA.size();i++){
                    List<Integer> min = minA.get(i);
                    for(List<Integer> max: maxA){
                        if(contain(min,max) && contain(x,max)){
                            System.out.println("CL: i="+i+ " size: "+  min.size());
                            if(!exists(min,CLResult)){
                                CLResult.add(min);                               
                            }
                        }
                    }             

                    List<Integer> intersect = intersection(min,x);
                    if (!intersect.isEmpty()){
                        System.out.print("CU: "+ intersect.size());                        
                        if(!exists(intersect,CUResult)){
                            CUResult.add(intersect);                          
                        }
                    }
                }
                System.out.println("size CLResult: "+ CLResult.size());
                System.out.println("size CUResult: "+ CUResult.size());
                // calculate possible lower approximation (PL) and possible upper approximation (PU)
                for(int i=0;i<maxA.size();i++){
                    List<Integer> max=maxA.get(i);
                    for(List<Integer> min: minA){
                        if(contain(max,min) && contain(x,min)){
                            List<Integer> intersect= intersection(max,x);
                            if (!intersect.isEmpty()){
                                System.out.println("PL: i="+i+" size= "+ intersect.size());
                                if(!exists(intersect,PLResult)){
                                    PLResult.add(intersect);                          
                                }
                            }
                        }
                    }
                    List<Integer> intersect = intersection(max,x);
                    if (!intersect.isEmpty()){
                        System.out.println("PU: "+ intersect.size());
                        if(!exists(intersect,PUResult)){
                            PUResult.add(intersect);                          
                        }
                    }
                }
            }
            // sort by length of the list
            Collections.sort(CLResult, new Comparator(){ 
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Comparable) ((List<Integer>) (o1)).size()).compareTo(((List<Integer>) (o2)).size());
                    }
                });
            Collections.sort(CUResult, new Comparator(){ 
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Comparable) ((List<Integer>) (o1)).size()).compareTo(((List<Integer>) (o2)).size());
                    }
                });
            Collections.sort(PLResult, new Comparator(){ 
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Comparable) ((List<Integer>) (o1)).size()).compareTo(((List<Integer>) (o2)).size());
                    }
                });
            Collections.sort(PUResult, new Comparator(){ 
                    @Override
                    public int compare(Object o1, Object o2) {
                        return ((Comparable) ((List<Integer>) (o1)).size()).compareTo(((List<Integer>) (o2)).size());
                    }
                });

            String temp;
            for(List<Integer> l: CLResult){
                temp = comma.join(l);
                bufferedCLWriter.write(temp);
                bufferedCLWriter.newLine();
            }

            for(List<Integer> l: CUResult){
                temp = comma.join(l);
                bufferedCUWriter.write(temp);
                bufferedCUWriter.newLine();
            }
            for(List<Integer> l: PLResult){
                temp = comma.join(l);                          
                bufferedPLWriter.write(temp);
                bufferedPLWriter.newLine();
            }
            for(List<Integer> l: PUResult){
                temp = comma.join(l);
                bufferedPUWriter.write(temp);
                bufferedPUWriter.newLine();
            }
            // Always close files.
            bufferedReader.close();
            bufferedCLWriter.close();bufferedCUWriter.close();bufferedPLWriter.close();bufferedPUWriter.close();
            long stopTime = System.nanoTime();
            System.out.println("Time" + TimeUnit.NANOSECONDS.toSeconds(startTime - stopTime));
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");                  
            // Or we could just do this: 
            // ex.printStackTrace();
        }     
    }

    // return True if arr1 contains arr2. Both arr1 and arr2 are sorted. 
    public static Boolean contain(List<Integer> arr1, List<Integer> arr2) {
        if (arr1.size() < arr2.size()) return false;
        for (int v: arr2){
            if (!arr1.contains(v)){
                return false;
            }
        }
        return true;            
    }
    // return intersection of arr1 and arr2. Both arr1 and arr2 are sorted.  
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

    public static List<List<Integer>> readAggregatedFile(String fileName)throws IOException{
        List<List<Integer>> temp = new ArrayList<List<Integer>>();

        try {       
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line = null;                 
            while((line = bufferedReader.readLine()) != null) {    
                if (line.isEmpty()) continue;
                StringTokenizer st = new StringTokenizer(line, ",");                  
                List<Integer> myList = new ArrayList<Integer>();
                while(st.hasMoreTokens()){
                    myList.add(Integer.parseInt(st.nextToken()));
                }
                temp.add(myList);           
            }
            bufferedReader.close();

        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");                  
        }     
        return temp;
    }
}
