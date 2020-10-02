from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from xml_converter import convert_Badges,convert_Posts,convert_Users
from configFile import config
from TagPreprocessing import Preprocess
from Calculation import CalculateTagCount,CalculateAnswerTime,CalculateYearTagCount,CalculateActiveUser
import psycopg2
conf = config()

def initializeSpark():
    sc = SparkContext()
    #sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
    return SparkSession(sc)


def RunSpark(spark):
    #df_Badges = convert_badges(spark,conf.s3file_Badges)
    Posts = convert_Posts(spark,conf.s3file_Posts)
    Questions,Answers = Preprocess(spark,Posts)
    Users = convert_Users(spark,conf.s3file_Users)
    #Preprocessing the post and Map tag into tag id
  
    bucket = conf.bucketparquet
    QuestionsWithAnswerTimePath = conf.generateS3Path(bucket,"QuestionsWithAnswerTime","parquet")
    MutualTagCountPath = conf.generateS3Path(bucket,"MutualTagCount","parquet")
    SingleTagCountPath = conf.generateS3Path(bucket,"SingleTagCount","parquet")
    YearTagCountPath = conf.generateS3Path(bucket,"YearTagCount","parquet")
    ActiveUsersPath = conf.generateS3Path(bucket,"ActiveUsers","parquet")
    
    QuestionsWithAnswerTime = CalculateAnswerTime(Questions,Answers) # calculate for once
    WriteToParquet(QuestionsWithAnswerTime,QuestionsWithAnswerTimePath)
    
    YearTagCount = CalculateYearTagCount(Questions)                  # calculate for once
    WriteToParquet(YearTagCount,YearTagCountPath)
    ActiveUsers=CalculateActiveUser(Questions,Answers,Users)         # calculate for once
    WriteToParquet(ActiveUsers,ActiveUsersPath)
    
    Questions = RangeSearchGetDataFrame(spark,"Questions",2011,2020)
    Answers = RangeSearchGetDataFrame(spark,"Answers",2011,2020)
    
    MutualTagCount,SingleTagCount = CalculateTagCount(Questions)
#     WriteToParquet(MutualTagCount,MutualTagCountPath)
#     WriteToParquet(SingleTagCount,SingleTagCountPath)
    
    
    
    
    
    
    
    
#     start = time.time()
#     Questions = RangeSearchGetDataFrame(spark,"Questions",2011,2020)
#     RangeSearchGetDataFrame_interval = time.time() -start
   
   
    
  
    
    return {'Questions':Questions,'Answers':Answers,"Users":Users}



def StopSpark(spark):
    spark.stop()


def RangeSearchGetDataFrame(spark,ObjectName,startYear,endYear,startMonth=None,endMonth=None):
    
    link = conf.LinkMap[ObjectName]
    parquetFile = spark.read.parquet(link)
    #parquetFile = spark.read.parquet(link+'/Year=*/Month=*/Day=*/Country=*')
    parquetFile.createOrReplaceTempView(ObjectName)
    sqlCommand = 'SELECT * from {}'.format(parquetFile)
    if not startMonth and not endMonth:
        sqlCommand = "SELECT * FROM {} WHERE Year >= {} AND Year <= {}".format(parquetFile,startYear,endYear,startMonth,endMonth)
    else:
        sqlCommand = "SELECT * FROM {} WHERE Year >= {} AND Year <= {} AND MONTH>={} AND MONTH<={}".format(parquetFile,startYear,endYear,startMonth,endMonth)
    return spark.sql(sqlCommand)


    
def WriteToParquet(df,link):
    
    df = df.repartition("Year", "Month")
    df.coalesce(8).write.format("parquet").partitionBy("Year", "Month").mode("overwrite").save(link)
    df.write.mode('overwrite').parquet(link)
    
if __name__=='__main__':
    spark = initializeSpark()
    RunSpark(spark)
    StopSpark(spark)
