from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from xml_converter import convert_Badges,convert_Posts,convert_Users
from configFile import config
from TagPreprocessing import Preprocess
from Calculation import *
import psycopg2
conf = config()

def initializeSpark():
    sc = SparkContext()
    #sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
    return SparkSession(sc)


def RunSpark(spark):
    
    
    # Get dataframe for post, question, answers and user
    Posts = convert_Posts(spark,conf.s3file_Posts)
    Questions,Answers = Preprocess(spark,Posts)
    Users = convert_Users(spark,conf.s3file_Users)
    
    # Generate Path to S3
    bucket = conf.bucketparquet
    QuestionsWithAnswerTimePath = conf.generateS3Path(bucket,"QuestionsWithAnswerTime","parquet")
    MutualTagCountPath = conf.generateS3Path(bucket,"MutualTagCount","parquet")
    SingleTagCountPath = conf.generateS3Path(bucket,"SingleTagCount","parquet")
    YearTagCountPath = conf.generateS3Path(bucket,"YearTagCount","parquet")
    ActiveUsersPath = conf.generateS3Path(bucket,"ActiveUsers","parquet")
    prefixSumYearCountPath = conf.generateS3Path(bucket,"prefixSumYearCount","parquet")
    
    
    ActiveUsers=CalculateActiveUser(Questions,Answers,Users)         # calculate for once

    WriteToParquet(ActiveUsers,ActiveUsersPath)
    QuestionsWithAnswerTime = CalculateAnswerTime(Questions,Answers) # calculate for once
    WriteToParquet(QuestionsWithAnswerTime,QuestionsWithAnswerTimePath)
    yearTagCount,prefixSumYearCount = CalculateYearTagCount(Questions)                  # calculate for once
  
    # Store yearTagCount and prefixSumYearCount
    
    
    
    
    
    # This is for user interface
#     beforeTable,afterTable = RangeSearchGetDataFrame(spark,prefixSumYearCountPath,2015,2020)
#     rangeYearTagCount = CalculateRangeYearTagCount2(beforeTable,afterTable)
#     singleTagCount,edge = CalculateSingleTagCount(rangeYearTagCount)
    

   
    
  
    return None
    #return {'Questions':Questions,'Answers':Answers,"Users":Users}



def StopSpark(spark):
    spark.stop()


def RangeSearchGetDataFrame(spark,link,startYear,endYear):   
    parquetFileBefore = spark.read.parquet(link+f'/Year={startYear}')
    parquetFileAfter = spark.read.parquet(link+f'/Year={endYear}')
    return parquetFileBefore,parquetFileAfter




    
def WriteToParquet(df,link):
    
    df = df.repartition("Year")
    df.coalesce(8).write.format("parquet").partitionBy("Year").mode("overwrite").save(link)

    
def writeToJDBC(df,tableName):
    #df.table(tableName).write.jdbc(config.jdbcUrl,tableName,config.connectionProperties)
    #df = df.na.fill(0)
    mode= "overwrite"
    df.write.jdbc(url=conf.jdbcUrl, table=tableName, mode=mode, properties=conf.connectionProperties)
    return tableNameList


      
def readFromJDBC(spark,tableName):
    jdbcDF = spark.read.format("jdbc") \
    .option("url", conf.jdbcUrl) \
    .option("dbtable", tableName) \
    .option("user", conf.username) \
    .option("password", conf.password) \
    .option("driver", "org.postgresql.Driver") \
    .option("partitionColumn", "Year")\
    .option("lowerBound", 1)\
    .option("upperBound", 4000000)\
    .option("numPartitions",100)\
    .load()
    return jdbcDF

    
if __name__=='__main__':
    spark = initializeSpark()
    RunSpark(spark)
    StopSpark(spark)
