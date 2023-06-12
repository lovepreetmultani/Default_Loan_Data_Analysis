package loanFiles;

//the base class for exceptions thrown while accessing the information using streams, files and directories
import java.io.IOException;

//
import java.util.ArrayList;

//writable class that wraps java long  
import org.apache.hadoop.io.LongWritable;

//to handle text
import org.apache.hadoop.io.Text;

//to implement the map tasks in hadoop mapreduce
import org.apache.hadoop.mapreduce.Reducer;

//Input key: Text, Input Value: LongWritable
//Output key: Text, Output Value: Text
public class LoanAmountReducer_1 extends Reducer<Text, LongWritable, Text, Text> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		//An arraylist called list and datatype long to store elements
		ArrayList<Long> list = new ArrayList<Long>();
		
		//To initialize a variable named SumofLoanAmount of type long to store the sum
		long SumofLoanAmount = 0;
		
		//To initialize a variable named average of type long to store the average value
		long average = 0;
		
		//The for loop to go through all the values passed to reducer from the LoanAmountMapper_1
		//calculate avgLoanAmount of all the loanAmount values for the corresponding employee title
		for (LongWritable value : values) {
			list.add(value.get());
			SumofLoanAmount+=value.get(); 
		}
		
		//to calcualte the elements present in the arraylist
		int size = list.size(); 

		//if condition to avoid null pointer, here checking the list size and sum of loan amount should be greater than zero 
		 if(SumofLoanAmount > 0 && size > 0)
		 {
			 //to clear the elements from the list.
			 list.clear();
			 
			 //calculating the average amount of loan taken per employee title
			 average=SumofLoanAmount/size;
		 }
		
		//write the empTitile and maxLoanAmount of loanAmount to the output
		context.write(new Text(key),new Text(""+average));
	}
}