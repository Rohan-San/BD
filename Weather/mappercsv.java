package weather;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter r) throws IOException {
                String line = value.toString();
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                        String year = parts[0].trim();
                        try {
                                Double temp = Double.parseDouble(parts[3].trim());
                                output.collect(new Text(year), new DoubleWritable(temp));
                        } catch (NumberFormatException e) {
                        }
                }
        }       
} 
