package oddeven; 
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
    int sum=0, count=0;
    while(value.hasNext()) {
      sum+=value.next().get();
      count++;
    }
    output.collect(new Text("Sum of "+key+" numbers"),new IntWritable(sum));
    output.collect(new Text(key+" NUmber count"),new IntWritable(count));
  }
}

// Typed out_
// package oddeven;
// import java.io.*;
// import java.util.*;
// import org.apache.hadoop.mapred.*;
// import org.apache.hadoop.io.*;

// public class reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
//         public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text,IntWritable> output, Reporter r) throws IOException {
//                 int sum = 0, count = 0;
//                 while(value.hasNext()) {
//                         sum+=value.next().get();
//                         count++;
//                 }
//                 output.collect(new Text("Sum of "+key+" numbers: "), new IntWritable(sum));
//                 output.collect(new Text(key+" number count"), new IntWritable(count));
//         }
// }
