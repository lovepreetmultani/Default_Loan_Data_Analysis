package loanFiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class LoanDetectionDriver {

	public static void main(String[] args) throws Exception {
		
		//create the “conf” object from the “Configuration” class, which provides access to
		 //configuration parameters necessary for Hadoop job
		Configuration conf = new Configuration();
		
		//if condition is to check if arguments is correct for properly executing the hadoop
		//and if the number of arguments is not 6, then, print message to
		 //the user that 6 arguments are needed and exit
		if (args.length != 6) {
			System.err.println("Usage: Loan Detection <input path> <output path>");
			System.exit(-1);
		}
		
		//job1 is created for loanAmountMapper and loanAmountReducer
		Job job1;

		 //configure parameters for the job1
		job1 = Job.getInstance(conf, "Employee Title");
		
		 //specifying the driver class in the Jar file
		job1.setJarByClass(LoanDetectionDriver.class);
		
		//setting the input and output paths for the job
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[3]));
		
		//setting the mappers and reducer classes for the first job
		job1.setMapperClass(LoanAmountMapper_1.class);
		job1.setReducerClass(LoanAmountReducer_1.class);
		
		//setting the output key and value classes for the first job
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		
		//waiting for job1 for completion
		job1.waitForCompletion(true);

		//job2 is created for salaryMapper and salaryReducer
		Job job2;

		 //configure parameters for the job2
		job2 = Job.getInstance(conf, "Employee Max Loan");
		
		 //specifying the driver class in the Jar file
		job2.setJarByClass(LoanDetectionDriver.class); 
		
		//setting the input and output paths for the job
		FileInputFormat.addInputPath(job2,new Path(args[2])); 
		FileOutputFormat.setOutputPath(job2, new Path(args[4]));
		
		//setting the Mapper class, Reducer class for second job
		job2.setMapperClass(SalaryMapper_1.class);
		job2.setReducerClass(SalaryReducer_1.class);
		
		//setting the output key and value classes for the second job
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class); 

		//waiting for job2 for completion
		job2.waitForCompletion(true);

		//job is created for LoanAmountMapper_2, SalaryMapper_2, and combinedoutputReducer
		Job job3;
		
		 //configure parameters for the job3
		job3=Job.getInstance(conf, "Employee Combined Result");
		
		 //specifying the driver class in the Jar file
		job3.setJarByClass(LoanDetectionDriver.class);

		//setting the mappers and reducer classes for the third job
		job3.setMapperClass(SalaryMapper_2.class);
		job3.setMapperClass(LoanAmountMapper_2.class);
		job3.setReducerClass(CombinedOutputReducer.class);

		//setting the Mapper output, Reducer output, output key and value classes for the third job
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		//adding the input paths and input format for the third job
		MultipleInputs.addInputPath(job3, new Path(args[3]), TextInputFormat.class, LoanAmountMapper_2.class);
		MultipleInputs.addInputPath(job3, new Path(args[4]), TextInputFormat.class, SalaryMapper_2.class);

		//adding the output path  for the third job
		FileOutputFormat.setOutputPath(job3, new Path(args[5]));
		
		// Delete output if exists. This part of is code is used to make sure that the output folder
		 // does not already exist in the hdfs because the program will create it anyway.
		 FileSystem hdfs = FileSystem.get(conf);
		 Path outputDir = new Path(args[5]);
		 if (hdfs.exists(outputDir))
		 hdfs.delete(outputDir, true);
		 
		//check the status of the job (completed or not)
		 System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}


