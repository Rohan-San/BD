package employee;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter r) throws IOException {
      String[] line = value.toString().split("\\t");
      Double salary = Double.parseDouble(line[8]);
      String firstName = line[1];
      output.collect(new Text(line[7].substring(0, 2)), new Text("S:" + salary)); // Emitting salary as Text
      output.collect(new Text(line[7].substring(0, 2)), new Text("N:" + firstName + ", S:" + salary));
  }
}
