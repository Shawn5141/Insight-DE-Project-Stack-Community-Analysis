# Introduction to s3_to_spark folder

s3_to_spark is folder store all the process for runing batch spark job to address Stack Overflow data store at s3. Function of each program can be viewed below.
- main.py : entrypoint of program
- configFile.py : store all the important configuration 
- runSpark.py   : call all the neccesary method
- xml_converter.py : convert xml data to spark dataframe
- TagPreprocessing.py : preprocessing tag data
- Calculation.py: table calculation
- dataModel.py  : method handle talking to database

# How to run the process:

1. Make sure you are acessing master node first.
2. Run this command `./shell_script/start_spark.sh` at s3_to_spark folder. It will trigger spark job

