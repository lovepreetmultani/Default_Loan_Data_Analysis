Default Loan Data using Map Reduce and Java

By utilizing Hadoop to extract information on average loan amounts, maximum salaries, and minimum salaries for each employee title from the given dataset filtered by loan status = “Default” to address the below research question

What is the financial risk associated with lending to borrowers with different employee titles? and what is the salary range for these defaulters?

The insights gained from this analysis can help a bank or loan lender identify any potential financial risks associated with lending to borrowers of different employee titles. Additionally, the salary range for these defaulters can provide valuable information on the income levels of those borrowers who have defaulted on their loans. This information can help the bank or loan lender refine their lending policies and adjust their lending practices to reduce the risk of default in the future.

Implementation:
We have followed the following step to start the hadoop services and see the output.

The below command is used to format the name node as it will remove all the previous data from hadoop file system.
hadoop namenode -format

Next the commands below are used to start the hadoop and to check if all instances of hadoop processed are working properly.
start-all.sh

We can create the HDFS directory needed to execute the MapReduce jobsThen, copy the content of input file data i.e. LoanData.txt from local file storage into distributed filesystem 
hdfs dfs -mkdir/input

Now we will execute the below command to run our local jar to the files in input folder of hadoop. 
hadoop jar Loan-1.0-SNAPSHOT.jar LoanDetectionDriver /input/LoanData.txt /output

Then, copy the output files from the distributed filesystem (HDFS) to the local filesystem 
hadoop dfs -get output

