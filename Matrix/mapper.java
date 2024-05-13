package matrix;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text,Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter r) throws IOException {
        String line[]=value.toString().split(",");
        Text OutputKey=new Text();
        Text OutputValue=new Text();
        if(line[0].equals("A")) {
            for(int i=0;i<3;i++) {
                OutputKey.set(line[1]+","+i);
                OutputValue.set("A,"+line[2]+","+line[3]);
                output.collect(OutputKey,OutputValue);
            }
        } else {
            for(int i=0;i<2;i++) {
                OutputKey.set(i+","+line[2]);
                OutputValue.set("B,"+line[1]+","+line[3]);
                output.collect(OutputKey,OutputValue);
            }
        }
    }
}
