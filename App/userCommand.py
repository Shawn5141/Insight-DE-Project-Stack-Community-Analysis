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
def getRangeYearTag(args):
    if args.year_list:
        #beginYear,endYear = args.year[0],args.year[1]
        beginYear,endYear = args.year_list.split(',')
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    bucket = conf.bucketparquet
    prefixSumYearCountPath = conf.generateS3Path(bucket,"prefixSumYearCount","parquet")
    beforeTable,afterTable = RangeSearchGetDataFrame(spark,prefixSumYearCountPath,beforeYear,endYear)
    rangeYearTagCount = CalculateRangeYearTagCount2(beforeTable,afterTable,sys.argv[3]) #filter number to get larger tag
    singleTagCount,edge = CalculateSingleTagCount(rangeYearTagCount)
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    singleTagCount.toPandas().to_csv("singleTagCount.csv", header=True)
    edge.toPandas().to_csv("edge.csv", header=True)
    spark.stop()

def getActiveUser(args):
    if len(args.year_list.split(','))>1:
        print("return",len(args.year_list.split(',')),args.year_list)
        return 
    else:
        year = args.year_list
    tags = args
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    bucket = conf.bucketparquet
    ActiveUsersPath = conf.generateS3Path(bucket,"ActiveUsers","parquet")
    df  = spark.read.parquet(ActiveUsersPath+f'/Year={year}')
    arr = f.array(*[f.lit(e) for e in args.list.split(',') if e])
    
    df = df.filter(df.Tags==arr).select("UserId","DisplayName","User_count_per_tag").sort(desc("User_count_per_tag"))
    df.show()
    return df
    
def getTagYearCount(args):
    if args.pid:
        pid = args.pid
    if args.path:
        path = args.path
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    bucket = conf.bucketparquet
    yearTagCountPath = conf.generateS3Path(bucket,"YearTagCount","parquet")
    yearTagCount = getWholeDataFrame(spark,yearTagCountPath)
    arr = f.array(*[f.lit(e) for e in args.list.split(',') if e])
    
    yearTagCount = yearTagCount.filter(yearTagCount.Tags==arr).select("Tags","Year","count").sort("Year")
    yearTagCount.show()
    if args.path:
        yearTagCount.toPandas().to_csv(path,header=True)
    spark.stop()
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='user command')
    #parser.add_argument('-y', '--year', nargs='+', type=int)
    parser.add_argument('-y','--year-list', type=str, help='year range')
    parser.add_argument('--pid' ,type=int, help='parent pid')
    parser.add_argument('--method' ,type=int, help='which method to implement')
    #parser.add_argument('--tagList', type=str,nargs='+', help='array of tags')
    parser.add_argument('-l', '--list', help='delimited list input', type=str)
    parser.add_argument('--path' ,type=str, help='where to store ')
    args = parser.parse_args()
    
    print(args)
    if args.method==1:
        getRangeYearTag(args) 
        
    if args.method==2:
        getTagYearCount(args)

    if args.method==3:
        getActiveUser(args)
    
    
    
  
    
    
   
    #yearTagCount.show(30,truncate=False)
    
#     singleTagCount.repartition(1).write.csv('./App/singleTagCount.csv', sep='|')
#     edge.repartition(1).write.csv('./App/edge.csv', sep='|')
    
#     edgeLink = conf.generateS3Path(bucket,"edge","parquet")
#     singleTagCountLink = conf.generateS3Path(bucket,"singleTagCount","parquet")
#     WriteToParquet(edge,edgeLink)
#     WriteToParquet(singleTagCount,singleTagCountLink)
    
    
    

    