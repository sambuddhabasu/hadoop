import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class HadoopAssignmentReducer
  extends Reducer<Text, Text, NullWritable, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

	  String[] output = new String[26];
	  System.out.println(key);

	  for (Text value : values) {
		  Integer place = new Integer(value.toString().split(",")[0]);
		  output[place - 1] = new String(value.toString().split(",")[1]);
	  }
	  
	  String finalOutput = new String(key.toString());
	  for(String x : output) {
		  finalOutput += "," + x.toString();
	  }
	  System.out.println(finalOutput);
	      
    context.write(null, new Text(finalOutput));
  }
}
