import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class HadoopAssignmentMapper
extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  // Generate the resumeFields key value pairs.
	  HashMap<String, Integer> resumeFields = new HashMap<String, Integer>();
	  resumeFields.put("Name", 1);
	  resumeFields.put("Email", 2);
	  resumeFields.put("Phone_No", 3);
	  resumeFields.put("Alternate_Phone", 4);
	  resumeFields.put("Years_Of_Exp", 5);
	  resumeFields.put("Location", 6);
	  resumeFields.put("Project_1_Description", 7);
	  resumeFields.put("Project_1_Role/Responsibility", 8);
	  resumeFields.put("Project_1_Environment", 9);
	  resumeFields.put("Project_1_Awards", 10);
	  resumeFields.put("Project_1_Start_Date", 11);
	  resumeFields.put("Project_1_End_Date", 12);
	  resumeFields.put("Project_2_Description", 13);
	  resumeFields.put("Project_2_Role/Responsibility", 14);
	  resumeFields.put("Project_2_Environment", 15);
	  resumeFields.put("Project_2_Awards", 16);
	  resumeFields.put("Project_2_Start_Date", 17);
	  resumeFields.put("Project_2_End_Date", 18);
	  resumeFields.put("Under_Grad_Degree", 19);
	  resumeFields.put("Under_Grad_University", 20);
	  resumeFields.put("Under_Grad_Passing_Year", 21);
	  resumeFields.put("Under_Grad_Cgpa", 22);
	  resumeFields.put("Post_Grad_Degree", 23);
	  resumeFields.put("Post_Grad_University", 24);
	  resumeFields.put("Post_Grad_Passing_Year", 25);
	  resumeFields.put("Post_Grad_Cgpa", 26);

	  // Generate the final key.
	  String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
	  int filePathSize = filePathString.split("/").length;
	  String finalKey = filePathString.split("/")[filePathSize - 1].split(".txt")[0];

	  // Generate the final value.
	  Integer getOrigKey = resumeFields.get(value.toString().split(":")[0]);
	  String getOrigValue = value.toString().split(":")[1];
	  String finalValue = getOrigKey.toString() + "," + getOrigValue;
	  System.out.println(finalValue);
	  
	  // Write the key, value pair to the context.
	  context.write(new Text(finalKey), new Text(finalValue));
  }
}
