

//the base class for exceptions thrown while accessing the information using streams, files and directories
import java.io.IOException;

//writable class that wraps java long  
import org.apache.hadoop.io.LongWritable;

//to handle text
import org.apache.hadoop.io.Text;

//to implement the map tasks in hadoop mapreduce
import org.apache.hadoop.mapreduce.Mapper;

public class SalaryMapper_2 extends Mapper<LongWritable, Text, Text, Text>{
	
	//create a object empTitle of type text 
    private Text empTitle = new Text();
    
	//create a object salary of type text 
    private Text salary = new Text();

	//create a mapper with three parameters
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
		//create line array of type String to hold the tqa escape separated value of the file
            String[] line = value.toString().split("\t");
            
    		//create variable jobTitle from line of type String to hold values of job title at 0th elements from line
            String jobtitle = line[0];
            
    		//set the value of jobtitle to empTitle
            empTitle.set(jobtitle);
            
    		//create fields array of type String to hold the comma separated value of the file
            String[] field = line[1].toString().split(",");
            
        	//create variable salaryAmount of type String to hold values of salary amount 0th element for
            //maximum salary and 1st element from field for minimum salary
    		//concatenate with String "salaryAmount" 
            String salaryAmount = "salaryAmount,"+field[0]+","+field[1];
            
    		//set the value of salaryAmount to salary
            salary.set(salaryAmount);

    		//write the empTitile and salary to the output
            context.write(empTitle,salary);
    }
}
