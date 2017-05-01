# Spark2.1 Pipeline Design

#### Wenliang Zhao (wenliang.zhao.nyu@gmail.com), Courant Institute

## Pipeline
1. Data reader:  
	I use Databricks [[spark-csv](https://github.com/databricks/spark-csv)] to read csv data. Spark version: [[Spark-2.1.0](http://spark.apache.org/releases/spark-release-2-1-0.html)]
2. Remove duplicate timestamp in events. 
	Group by advertizer id, user id and event type, sort by timestamp. Check each record sequentially to see if the current one is within 1 minute of the previous one.	
3. Convert events and impression to a generalized record format [GeneralRecord1]
	
4. Merge events and impressions so that we can compare timestamps together
	
5. Count attribution and generate a standard form of attribution dataset [GeneralAttribute]
	Group by aid and uid. This process collects two list: timestamp and event type. Sort these two lists by timestamp, then count for attribution based on the rule.  

6. Count for attribution for each advertizer and event type from standard attribution dataset
	Group by aid and etype, sum all counts. 

7. Count for unique user for each advertizer and event type from standard attribution dataset. 
	Group by aid and etype, collect uid into a list, get unique count of the list.


## Code Structure
1. Summarize.scala: main file contains all pipeline.  
2. Parameters.scala: object storing original dataset schema, input file paths, case classes pool.   
3. DeDuplication.scala: object with methods working on de-duplication of events.  
4. Attribution.scala: object with methods working on attribution.  


## Testing
I include 2 kinds of testing:  
1) [scalatest](http://www.scalatest.org/). 
	scalatest in this project is used for unit test on utilities functions. We use the style "FlatSpec + Matchers".  
2) [spark-testing-base](https://github.com/holdenk/spark-testing-base).    

Source Files:  
1. DeDuplicationMethodTest.scala: unit test for de-duplication methods   
2. DeDuplicationDatasetTest.scala: unit test for de-duplication dataset operations.  
3. AttributionMethodTest.scala: unit test for attribution methods.  
4. AttributionDatasetTest.scala: unit test for attribution dataset operations.  
5. PipelineIntegrationTest: integration test of whole pipeline process using testing files.  
  
#### Code test coverage. 
1. Statement coverage: 84.54%. 
2. Branch coverage: 100%
	

## Run code

There are two ways to run the code:

1. sbt:  
	Use runMain (or simply run) in sbt to run the code. Need to specify 2 parameters for the main function - event_file, impression_file.
2. assembly jar.   
	Use sbt assembly to pack the source code into a jar file, and run it accordingly.
	There is a python code "run.py" which automatically run everything. There are 5 steps in run.py
		
		2.1. Remove results from previous run (data/output_file, metastore_db)
		2.2. Clean files due to previous assembly
		2.3. pack the source code using assembly
		2.4. delete META-INF related unrelated files in jar using zip
		2.5. Run the code. 


**NOTICE:** If there is new data other then the testing files, user needs to change paths in run.py
