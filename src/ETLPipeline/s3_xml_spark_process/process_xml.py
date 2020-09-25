from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from xml_converter import convert_badges
from configFile import config
import psycopg2
conf = config()

def initializeSpark():
    sc = SparkContext()
    #sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
    return SparkSession(sc)
def RunSpark(spark):
    df = convert_badges(spark,conf.s3file)
    df.show()
    #print(df.collect()) 
    return df

def StopSpark(spark):
    spark.stop()

if __name__=='__main__':
    spark = initializeSpark()
    RunSpark(spark)
    StopSpark(spark)
