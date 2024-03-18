package wordcount; 
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

class reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
    int count=0;
    while(value.hasNext()) {
      count+=value.next().get();
    }
    output.collect(new Text(key),new IntWritable(count));
  }
}
