import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import javax.security.auth.login.Configuration;

import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;

public class HadoopAssignment {

	public static void readDocxFile(File workDir, File newWorkDir, File fileName) {
		try {
			
			// Read the contents of the docx file.
			FileInputStream fis = new FileInputStream(fileName.getAbsolutePath());

			XWPFDocument document = new XWPFDocument(fis);

			List<XWPFParagraph> paragraphs = document.getParagraphs();
			
			String resumeFileName = fileName.getName().split(".docx")[0];
			
			String newResumeFileName = newWorkDir + "/" + resumeFileName + ".txt";
			
			System.out.println(newResumeFileName);
			
			// Write readable data to the txt file.
			PrintWriter writer = new PrintWriter(newResumeFileName);
			for (XWPFParagraph para : paragraphs) {
				System.out.println(para.getText());
				writer.println(para.getText());
			}
			writer.close();
			fis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: HadoopAssignment <input path> <output path>");
			System.exit(-1);
		}
		
		
		// Create the new working directory.
		File workDir = new File(args[0]);
		File newWorkDir = new File(workDir.getParentFile().getPath().toString() + "/newResume");
		if(!newWorkDir.exists()) {
			newWorkDir.mkdir();
		}
		
		// Convert the docx files to txt.
		File[] origResume = workDir.listFiles();
		for(int i = 0; i < origResume.length; i++) {
			File resumePath = new File(workDir, origResume[i].getName());
			System.out.println(resumePath);
			readDocxFile(workDir, newWorkDir, resumePath);
		}

		// Create the new job and set its properties.
		Job job = new Job();
		job.setJarByClass(HadoopAssignment.class);
		job.setJobName("Hadoop Assignment");
		
		FileInputFormat.addInputPath(job, new Path(newWorkDir.toString()));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(HadoopAssignmentMapper.class);
		job.setReducerClass(HadoopAssignmentReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}