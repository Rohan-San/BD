package sales;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private String uname;

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
                String[] line = value.toString().split(",");
                String unm = line[4];
                if (unm.equals("carolina")) {
                        int price = Integer.parseInt(line[2]);
                        String prod = line[1];
                        output.collect(new Text("Name "+unm), new IntWritable(price));
                        output.collect(new Text("Product "+prod), new IntWritable(price));
                }
        }
}
