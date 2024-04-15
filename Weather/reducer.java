package weather;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

class reducer extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        public void reduce(Text key, Iterator<DoubleWritable> value, OutputCollector<Text,DoubleWritable> output, Reporter r) throws IOException {
                Double max = -9999.0;
                Double min = 9999.0;
                while(value.hasNext()) {
                        Double temp = value.next().get();
                        max = Math.max(max, temp);
                        min = Math.min(min, temp);
                }       
                output.collect(new Text("Max temp at "+key), new DoubleWritable(max));
                output.collect(new Text("Min temp at "+key), new DoubleWritable(min));
        }       
} 
