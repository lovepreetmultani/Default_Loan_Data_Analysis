

//the base class for exceptions thrown while accessing the information using streams, files and directories
import java.io.IOException;

//writable class that wraps java long  
import org.apache.hadoop.io.LongWritable;

//to handle text
import org.apache.hadoop.io.Text;

//to implement the map tasks in hadoop mapreduce
import org.apache.hadoop.mapreduce.Mapper;

public class LoanAmountMapper_1 extends Mapper<LongWritable, Text, Text, LongWritable> {

	//create a object empTitle of type text 
	private Text empTitle = new Text();
	
	//create object called tempLongWritable from a class LongWritable.
	//It will be used for the loan amount in field 3 in the input data
	private final static LongWritable tempLongWritable = new LongWritable(0);

	//create a mapper with three parameters
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//create fields array of type String to hold the comma separated value of the file
		String[] fields = value.toString().split(",");

		//create variable loan status from fields of type String to hold values of loan status 7th elements from fields
		String loanStatus = fields[7].trim();
		
		//create variable jobTitle from fields of type String to hold values of job title 5th elements from fields
		String jobTitle = fields[5].trim();
		
		//create variable tempLoanAmount of type Long to hold values of loan amount 3rd element from fields
		Long tempLoanAmount = Long.parseLong(fields[3].trim());
		
		//if condition to check only the emp title with only default loans
		if (loanStatus.equals("Default")) {
			
			//set the value of the object empTitle of type String to hold values of jobTitle.
			empTitle.set(jobTitle.toLowerCase());
			
			//set the value of the object tempLongWritable of type LongWritable to hold values of tempLoanAmount.
			tempLongWritable.set(tempLoanAmount);
			
			//write the empTitile and loan amount(tempLongWritable) to the output
			context.write(empTitle, tempLongWritable);
		}
	}
}