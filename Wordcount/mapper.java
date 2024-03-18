package wordcount;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
    String line[]=value.toString().split(" "); 
    for(String a:line) {
      output.collect(new Text(a),new IntWritable(1));
    }
  }
}
