
import sys
import os
sys.path.append(os.getcwd())
from s3_to_spark.Calculation import *
from s3_to_spark.configFile import *

def WriteToParquet(df,link):
    
    df.write.format("parquet").mode("overwrite").save(link)

if __name__=="__main__":
    conf = config()
    print("working",sys.argv)
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
#     singleTagCount.repartition(1).write.csv('./App/singleTagCount.csv', sep='|')
#     edge.repartition(1).write.csv('./App/edge.csv', sep='|')
    
#     edgeLink = conf.generateS3Path(bucket,"edge","parquet")
#     singleTagCountLink = conf.generateS3Path(bucket,"singleTagCount","parquet")
#     WriteToParquet(edge,edgeLink)
#     WriteToParquet(singleTagCount,singleTagCountLink)
    
    spark.stop()
    

    