package loanFiles;

//the base class for exceptions thrown while accessing the information using streams, files and directories
import java.io.IOException;

//to read or write double values  
import org.apache.hadoop.io.DoubleWritable;

//writable class that wraps java long  
import org.apache.hadoop.io.LongWritable;

//to handle text
import org.apache.hadoop.io.Text;

//to implement the map tasks in hadoop mapreduce
import org.apache.hadoop.mapreduce.Mapper;

public class SalaryMapper_1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	//create a object empTitle of type text 
	private Text empTitle = new Text();

	//create object called tempDoubleWritable from a class DoubleWritable.
	//It will be used for the salary amount in field 6 in the input data
	private final static DoubleWritable tempDoubleWritable = new DoubleWritable(0);

	//create a mapper with three parameters
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//create fields array of type String to hold the comma separated value of the file
		String[] fields = value.toString().split(",");

		//create variable loan status from fields of type String to hold values of loan status 7th elements from fields
		String loanStatus = fields[7].trim();

		//create variable jobTitle from fields of type String to hold values of loan status 5th elements from fields
		String jobTitle = fields[5].trim();

		//create variable tempSalaryAmount of type Long to hold values of values of salary amount 6th element from fields
		Double tempSalaryAmount =Double.parseDouble(fields[6].trim());

		//if condition to check only the emp title with only default loans
		if (loanStatus.equals("Default")) {

			//set the value of the object empTitle of type String to hold values of jobTitle.
			empTitle.set(jobTitle.toLowerCase());

			//set the value of the object tempDoubleWritable of type DoubleWritable to hold values of tempSalaryAmount.
			tempDoubleWritable.set(tempSalaryAmount);

			//write the empTitile and Salary amount(tempDoubleWritable) to the output
			context.write(empTitle, tempDoubleWritable);
		}
	}
}