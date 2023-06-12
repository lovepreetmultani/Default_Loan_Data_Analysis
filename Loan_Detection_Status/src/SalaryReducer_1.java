

//the base class for exceptions thrown while accessing the information using streams, files and directories
import java.io.IOException;

//to read or write double values
import org.apache.hadoop.io.DoubleWritable;

//to handle text
import org.apache.hadoop.io.Text;

//to implement the reduce tasks in hadoop mapreduce
import org.apache.hadoop.mapreduce.Reducer;

//Input key: Text, Input Value: DoubleWritable
//Output key: Text, Output Value: Text
public class SalaryReducer_1 extends Reducer<Text, DoubleWritable, Text, Text> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		//To initialize a variable named maxValue of type double with the minimum possible value of an integer
		double maxValue=Integer.MIN_VALUE;
		
		//To initialize a variable named maxValue of type double with the maximum possible value of an integer
		double minValue=Integer.MAX_VALUE;
		
		//The for loop to go through all the values passed to reducer from the salaryMapper_1
		//calculate minValue and maxValue from the salary values for the corresponding employee title
		for (DoubleWritable value : values) {
			//Math.max() method is used to compare the current value of maxValue 
			//with the value returned by value.get() and return the greater of the two.
			maxValue = Math.max(maxValue, value.get());
			
			//Math.min() method is used to compare the current value of minValue 
			//with the value returned by value.get() and return the greater of the two.
			minValue = Math.min(minValue, value.get());
		}
		
		//write the empTitile and minValue and maxValue of salary to the output
		context.write(new Text(key),new Text((minValue +","+ maxValue)));
	}
}
