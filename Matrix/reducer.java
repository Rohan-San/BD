package matrix;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class reducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
    public void reduce(Text key ,Iterator<Text> value , OutputCollector<Text,Text> output,Reporter r) throws IOException {
        HashMap<Integer,Float> a=new HashMap<Integer,Float>();
        HashMap<Integer,Float> b=new HashMap<Integer,Float>();
        String[] v;
        while(value.hasNext()) {
            v=value.next().toString().split(",");
            if(v[0].equals("A")) {
            a.put(Integer.parseInt(v[1]),Float.parseFloat(v[2]));
            }
            else {
            b.put(Integer.parseInt(v[1]),Float.parseFloat(v[2]));
            }
        }
        float aij,bij, result=0.0f;
        for(int i=0;i<5;i++) {
            aij=a.containsKey(i) ? a.get(i): 0.0f;
            bij=b.containsKey(i) ? b.get(i): 0.0f;
            result+=aij*bij;
        }
        if(result!=0.0f) {
            output.collect(null,new Text(key+","+Float.toString(result)));
        }
    }
}
