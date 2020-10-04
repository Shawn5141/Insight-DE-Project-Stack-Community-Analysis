from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession



bucketparquet = 'stackoverflowparquet'
Badges_Parquet,Comments_Parquet,PostHistory_Parquet,PostLinks_Parquet,Posts_Parquet,Answers_Parquet ,Tags_Parquet,Users_Parquet,Votes_Parquet= 'Badges.parquet','Comments.parquet','PostHistory.parquet','PostLinks.parquet','Posts.parquet','Answers.Parquet','Tags.parquet','Users.parquet','Votes.parquet'
s3file_parquet_Badges = f's3a://{bucketparquet}/{Badges_Parquet}' 
s3file_parquet_Comments = f's3a://{bucketparquet}/{Comments_Parquet}' 
s3file_parquet_PostHistory = f's3a://{bucketparquet}/{PostHistory_Parquet}' 
s3file_parquet_PostLinks = f's3a://{bucketparquet}/{PostLinks_Parquet}' 
s3file_parquet_Posts = f's3a://{bucketparquet}/{Posts_Parquet}' 
s3file_parquet_Answers = f's3a://{bucketparquet}/{Answers_Parquet}' 
s3file_parquet_Tags = f's3a://{bucketparquet}/{Tags_Parquet}' 
s3file_parquet_Users = f's3a://{bucketparquet}/{Users_Parquet}'
s3file_parquet_Votes = f's3a://{bucketparquet}/{Votes_Parquet}'

# Parquet files can also be used to create a temporary view and then used in SQL statements.

# spark = SparkSession.builder.getOrCreate()
# parquetFile = spark.read.parquet(s3file_parquet_Posts)
# parquetFile.createOrReplaceTempView("parquetFile")
# teenagers = spark.sql("SELECT id,Title FROM parquetFile")
# teenagers.show(n=5, truncate=False, vertical=True)


from pyspark.sql import Window
schema = StructType([StructField("Tags", ArrayType( StringType()), True),\
                     StructField("mappingResult", ArrayType(StringType()), True),\
                     StructField("Year", IntegerType(), True),\
                     StructField("Month", IntegerType(), True)])
                                 
appName = "PySpark Example"
spark = SparkSession.builder \
    .appName(appName) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
Questions=spark.createDataFrame([(['tag1','tag2'],['1','2'],1991,1)\
                                 ,(['tag1','tag2'],['1','2'],1991,2)\
                                 ,(['tag1','tag2'],['1','2'],1991,3)\
                                     ,(['tag2','tag3'],['2','3'],1990,2)\
                                     ,(['tag2','tag3'],['2','3'],1992,2)\
                                     ,(['tag2','tag3'],['2','3'],1995,2)\
                                     ,(['tag2','tag4'],['2','4'],1991,4)\
                                     ,(['tag2','tag4'],['2','4'],1991,3)]\
                                     ,schema)

# tag1 tag2 = 3
# tag2 tga3 = 3
# tag2 tag4 = 3

BeginYear = 1991
EndYear =1995

cols = ["Tags", "mappingResult","Year"]
w = Window.partitionBy(cols)
new_Questions = Questions.withColumn("count", count("mappingResult").over(w)).orderBy("count").dropDuplicates(["Tags","Year"])
windowval = (Window.partitionBy('Tags').orderBy('Year').rangeBetween(Window.unboundedPreceding, 0))
new_Questions_cumSum = new_Questions.withColumn('cum_sum', sum('count').over(windowval)).drop(*['Month','count'])
cols = ["Tags"]
w = Window.partitionBy(cols)
new_Questions_cumSum = new_Questions_cumSum.withColumn("GroupCount", count("Tags").over(w))
new_Questions_cumSum.show()
w = Window.partitionBy("Tags")

new_Questions_cumSum=new_Questions_cumSum.filter((new_Questions_cumSum.Year>=BeginYear-1) & (new_Questions_cumSum.Year<=EndYear))
rangeYearTagCount = new_Questions_cumSum.withColumn('YearTagCount', when(col('GroupCount')>=2,max('cum_sum').over(w)- when(min('Year').over(w) == BeginYear-1,min('cum_sum').over(w)).otherwise(0)).otherwise(max('cum_sum').over(w))).dropDuplicates(["Tags"])

                                            
rangeYearTagCount = rangeYearTagCount.withColumn("Original_Tags",Questions.Tags)
df = rangeYearTagCount.select("Original_Tags",explode(rangeYearTagCount.Tags).alias('Tags'),col("YearTagCount"))
edge = df.select("Original_Tags","Tags")
singleTagCount=df.groupBy(df.Original_Tags,df.Tags).agg({"YearTagCount": "sum"}).withColumnRenamed("sum(YearTagCount)","YearTagCount_Sum").sort(desc("YearTagCount_Sum"))

singleTagCount.show(30, truncate = False)
edge.show(30, truncate = False)
 