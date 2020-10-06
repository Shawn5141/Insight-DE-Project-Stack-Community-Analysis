import sys
import os
sys.path.append(os.getcwd())
from s3_to_spark.Calculation import *
from s3_to_spark.configFile import *
import pyspark.sql.functions as f
import argparse

conf = config()
def WriteToParquet(df,link):
    
    df.write.format("parquet").mode("overwrite").save(link)
def getRangeYearTag():
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    bucket = conf.bucketparquet
    prefixSumYearCountPath = conf.generateS3Path(bucket,"prefixSumYearCount","parquet")
    beforeTable,afterTable = RangeSearchGetDataFrame(spark,prefixSumYearCountPath,sys.argv[1],sys.argv[2])
    rangeYearTagCount = CalculateRangeYearTagCount2(beforeTable,afterTable,sys.argv[3]) #filter number to get larger tag
    singleTagCount,edge = CalculateSingleTagCount(rangeYearTagCount)
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    singleTagCount.toPandas().to_csv("singleTagCount.csv", header=True)
    edge.toPandas().to_csv("edge.csv", header=True)
    spark.stop()

    
def getTagYearCount(args):
    pid = args.pid
    beginYear,endYear = args.yearRange.split(',')
    spark = SparkSession.builder.getOrCreate(path)
    spark.sparkContext.setLogLevel("WARN")
    bucket = conf.bucketparquet
    yearTagCountPath = conf.generateS3Path(bucket,"YearTagCount","parquet")
    yearTagCount = getWholeDataFrame(spark,yearTagCountPath)
    arr = f.array(*[f.lit(e) for e in TagList.split(',') if e])
    
    yearTagCount = yearTagCount.filter(yearTagCount.Tags==arr).select("Tags","Year","count").sort("Year")
   
    yearTagCount.toPandas().to_csv(path,header=True)
    spark.stop()
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='user command')
    parser.add_argument('yearRange','--year', type=str, help='year range')
    parser.add_argument('pid','--pid' ,type=int, help='parent pid')
    parser.add_argument('methodNum','--method' ,type=int, help='which method to implement')
    parser.add_argument('tagList','--tagList' ,type=int, help='array of tags')
    parser.add_argument('path','--path' ,type=str, help='where to store ')
    args = parser.parse_args()
    
    
    
    tagList = args.tagList
    if args.methodNum==1:
        getRangeYearTag(args) 
        
    if args.methodNum==2:
        getTagYearCount(args)

    
    
    
    
  
    
    
   
    #yearTagCount.show(30,truncate=False)
    
#     singleTagCount.repartition(1).write.csv('./App/singleTagCount.csv', sep='|')
#     edge.repartition(1).write.csv('./App/edge.csv', sep='|')
    
#     edgeLink = conf.generateS3Path(bucket,"edge","parquet")
#     singleTagCountLink = conf.generateS3Path(bucket,"singleTagCount","parquet")
#     WriteToParquet(edge,edgeLink)
#     WriteToParquet(singleTagCount,singleTagCountLink)
    
    
    

    