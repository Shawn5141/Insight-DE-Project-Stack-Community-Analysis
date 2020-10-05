#import findspark
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField
from datetime import datetime

appName = "PySpark Partition Example"
spark = SparkSession.builder \
    .appName(appName) \
    .getOrCreate()
print(spark.version)
def writeToS3(link): 
    start_date = date(2019, 1, 1)
    data = []
    for i in range(0, 1000):
        curr_date = start_date +timedelta(days=i)
        curr_date = curr_date.strftime("%Y-%m")  # from datetime to string
        #print(curr_date)
        curr_date=datetime.strptime(curr_date, "%Y-%m")  # from string to datetime
        #print(curr_date)
        data.append({"Country": "CN", "Date":curr_date , "Amount": 10+i})
        data.append({"Country": "AU", "Date": curr_date, "Amount": 10+i})


    schema = StructType([StructField('Country', StringType(), nullable=False),
                     StructField('Date', DateType(), nullable=False),
                     StructField('Amount', IntegerType(), nullable=False)])

    df = spark.createDataFrame(data, schema=schema)
    df.show()
    print(df.rdd.getNumPartitions())
    df = df.withColumn("Year", year("Date")).withColumn("Month", month("Date")).withColumn("Day", dayofmonth("Date"))
    df = df.repartition("Year", "Month", "Day", "Country")
    df.coalesce(8).write.format("parquet").partitionBy("Year","Month","Day","Country").mode("overwrite").save(link)
path = 'test.parquet'
bucketparquet = 'stackoverflowparquet'
link =  f's3a://{bucketparquet}/{path}'

# writeToS3(link)

def readFromS3(spark,link):
    parquetFile = spark.read.parquet(link)
    #parquetFile = spark.read.parquet(link+'/Year=*/Month=*/Day=*/Country=*')
    parquetFile.createOrReplaceTempView("parquetFile")
    #parquetFile.show()
    
    teenagers = spark.sql("SELECT * FROM parquetFile WHERE Year >= 2020 AND Year <= 2021").explain()
    teenagers.show()

readFromS3(spark,link)
