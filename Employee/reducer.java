package employee;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

class reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterator<DoubleWritable> value, OutputCollector<Text, DoubleWritable> output, Reporter r) throws IOException {
                int count = 0;
                Double sum = 0.0;
                while(value.hasNext()) {
                        sum += value.next().get();
                        count+=1;
                }
                output.collect(new Text(key+" average"), new DoubleWritable(sum/count));
                output.collect(new Text(key+" count"), new DoubleWritable(count));
        }
}
