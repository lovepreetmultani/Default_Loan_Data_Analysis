package loanFiles;

//the base class for exceptions thrown while accessing the information using streams, files and directories
import java.io.IOException;

//writable class that wraps java long  
import org.apache.hadoop.io.LongWritable;

//to handle text
import org.apache.hadoop.io.Text;

//to implement the map tasks in hadoop mapreduce
import org.apache.hadoop.mapreduce.Mapper;

public class LoanAmountMapper_2 extends Mapper<LongWritable, Text, Text, Text>{

	//create a object empTitle of type text 
	private Text empTitle = new Text();

	//create object called loanAmount from a type text.
	private Text loanAmount = new Text();

	//create a mapper with three parameters
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//create line array of type String to hold the tab escape separated value of the file
		String[] line = value.toString().split("\t");
		
		//create variable jobTitle from line of type String to hold values of job title at 0th elements from line
		String jobtitle = line[0];
		
		//set the value of jobtitle to empTitle
		empTitle.set(jobtitle);

		//create variable AvgLoanAmount of type String to hold values of loan amount 1st element from line
		//concatenate with String "LoanAmount" 
		String AvgLoanAmount = "LoanAmount,"+line[1].trim();
		
		//set the value of MaxLoanAmount to loanAmount
		loanAmount.set(AvgLoanAmount);

		//write the empTitile and loan amount(AvgLoanAmount) to the output
		context.write(empTitle,loanAmount);
	}
}
