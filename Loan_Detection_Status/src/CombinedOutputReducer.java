

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinedOutputReducer extends Reducer<Text, Text, Text, Text> {


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
		//NULL object to return null values
		final Text NULL = null;
		
		//create a  variable avgloanAmount to type String 
		String avgloanAmount=""; 
		
		//create a  variable salaryMax to type String 
		String salaryMax=""; 
		
		//create a  variable salaryMin to type String 
		String salaryMin="";

		//The for loop will go through all the maxloanAmount, salaryMin, and salaryMax values passed to the reducer
		for (Text value : values) {
			
			//create fields array of type String to hold the comma separated value of the file
			String[] field = value.toString().split(",");

			//if condition to check if the field is returning the result concatenated with salaryAmount or LoanAmount
			//on the basis of above we map the avgloanAmount, salaryMin and salaryMax to string values
			if(field[0].equals("salaryAmount")) { 
				salaryMin=field[1];
				salaryMax=field[2]; 
			}else if(field[0].equals("LoanAmount")){
				avgloanAmount=field[1]; 
			}
			
			//Since all 3 fields maxloanAmount, salaryMin, salaryMax are mandatory for our analysis.
			//so if condition is to check if all the fields are not null
			if(!salaryMin.trim().isEmpty() && !salaryMax.trim().isEmpty() && !avgloanAmount.trim().isEmpty()) {
				
				//write the emp title to the output
				context.write(new Text("Employee title is:"),new Text(key));
				
				//write the Average loan amount to the output
				context.write(new Text("Average Loan Amount:"),new Text(avgloanAmount));
				
				//write the minimum salary to the output
				context.write(new Text("Minimum Salary:"),new Text(salaryMin));
				
				//write the maximum salary to the output
				context.write(new Text("Maximum Salary:"),new Text(salaryMax));
				
				//write the text with symbol "--" to the output to visually show different results
				context.write(new Text("----------------------------------------------------------"),NULL);
			}
		}
	}
}
