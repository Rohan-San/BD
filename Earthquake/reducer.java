package earthquake;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

class reducer extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    public void reduce(Text key, Iterator<DoubleWritable> value, OutputCollector<Text,DoubleWritable> output, Reporter r) throws IOException {
         Double max=-9999.0;
         while(value.hasNext()) {
                 Double temp=value.next().get();
                 max=Math.max(max,temp);
         }
         output.collect(new Text(key), new DoubleWritable(max));
    }
}
