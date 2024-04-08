package oddeven;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
    String[] line = value.toString().split(" "); 
    for(String num:line) {
      int no = Integer.parseInt(num);
      if (no%2==0) {
        output.collect(new Text("even"), new IntWritable(no));
      }
      else {
        output.collect(new Text("odd"), new IntWritable(no));
    }
  }
}
