from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window






def RangeSearchGetDataFrame(spark,link,startYear,endYear):   
    parquetFileBefore = spark.read.parquet(link+f'/Year={startYear}')
    parquetFileAfter = spark.read.parquet(link+f'/Year={endYear}')
    return parquetFileBefore,parquetFileAfter

def CalculateTagCount(Questions):
    # MutualTagCount
    MutualTagCount = Questions.select("Tags","mappingResult").groupBy("mappingResult","Tags").count().withColumnRenamed("count","Tag Number").sort(desc("count"))
    # Single Tag Count
    df = MutualTagCount.select(explode(MutualTagCount.Tags).alias('Tags'),col("Tag Number"))
    SingleTagCount=df.groupBy(df.Tags).agg({"Tag Number": "sum"}).alias('Tag Number Sum').sort(desc("sum(Tag Number)"))
#     # Year Tag Count
#     YearTagCount = Questions.select("Tags","mappingResult","Year","Month").groupBy("mappingResult","Tags","Year","Month").count()\
#     .withColumnRenamed("count","Tag in each Year/Month")
    
    MutualTagCount.show()
    SingleTagCount.show()
#     YearTagCount.show()
    return MutualTagCount,SingleTagCount

def cumSum(df,windowval):
    #windowval = (Window.partitionBy('class').orderBy('time').rangeBetween(Window.unboundedPreceding, 0))
    df_w_cumsum = df.withColumn('cum_sum', F.sum('value').over(windowval))
    df_w_cumsum.show()


def CalculateYearTagCount(Questions):
    # Calculate for once
    
    cols = ["Tags", "mappingResult","Year"]
    w = Window.partitionBy(cols)
    YearCount = Questions.select("Tags","Year","Month","mappingResult").withColumn("count", count("mappingResult").over(w)).orderBy("count").dropDuplicates(["Tags","Year"]).drop('Month')
    YearCount.sort(desc('count')).show()
    windowval = (Window.partitionBy('Tags').orderBy('Year').rangeBetween(Window.unboundedPreceding, 0))
    PrefixSumYearCount = YearCount.withColumn('cum_sum', sum('count').over(windowval)).drop('count')
    PrefixSumYearCount.show()
    return YearCount,PrefixSumYearCount
    
def CalculateRangeYearTagCount(PrefixSumYearCount,BeginYear,EndYear):
    # Calculate based on precompute table
    
    cols = ["Tags"]
    w = Window.partitionBy(cols)
    df = PrefixSumYearCount.withColumn("GroupCount", count("Tags").over(w))
    w = Window.partitionBy("Tags")
    # Filter out based on begin year and end year
    df=df.filter((df.Year>=BeginYear-1) & (df.Year<=EndYear))
    rangeYearTagCount = df.withColumn('YearTagCount', when(col('GroupCount')>=2,max('cum_sum').over(w)- when(min('Year').over(w) == BeginYear-1,min('cum_sum').over(w)).otherwise(0)).otherwise(max('cum_sum').over(w))).dropDuplicates(["Tags"]).drop("Year").sort(desc('YearTagCount'))
    rangeYearTagCount.show(30, truncate = False)
    return rangeYearTagCount
    
def CalculateRangeYearTagCount2(preTable,postTable,filterNum):
    # Subtract post and pre and get 
    preTable = preTable.withColumnRenamed("cum_sum","pre_cum_sum").withColumnRenamed("Year","pre_Year").withColumnRenamed("mappingResult","pre_mappingResult").withColumnRenamed("Tags","pre_Tags")
    
    joinTable = postTable.join(preTable,postTable.Tags == preTable.pre_Tags,how='left')
    joinTable = joinTable.withColumn("YearTagCount",joinTable.cum_sum-when(joinTable.pre_cum_sum.isNull(),0).otherwise(joinTable.pre_cum_sum))
    return joinTable.select("Tags","YearTagCount").filter(joinTable.YearTagCount>filterNum)


def CalculateSingleTagCount(rangeYearTagCount):
    def getter(column):
        col_new=''
        if len(column)==1:
            return None
        for i,col in enumerate(column):
            if i==0:
               col_new=col
            else:
               col_new=col_new+','+col
        return col_new

    getterUDF = udf(getter, StringType())

    rangeYearTagCount = rangeYearTagCount.withColumn("Original_Tags",rangeYearTagCount.Tags)
    df = rangeYearTagCount.select(getterUDF(col("Original_Tags")).alias("Original_Tags"),explode(rangeYearTagCount.Tags).alias('Tags'),col("YearTagCount"))
    edge = df.filter(col("Original_Tags").isNotNull()).select("Original_Tags","Tags","YearTagCount")
    
    singleTagCount=df.groupBy(df.Tags).agg({"YearTagCount": "sum"}).withColumnRenamed("sum(YearTagCount)","singleTagCount")
    singleTagCount.show(10, truncate = False)
    edge.show(10,truncate=False)
    return singleTagCount,edge

    
def CalculateAnswerTime(Questions,Answers):
#     Answers.show()
#     Questions.show()
    Answers = Answers.select(col('ParentId'),col('Id').alias('AnswersID'),\
                                 col('CreationDate').alias('AnswersCreationDate'))
                                           
    Answers = Answers.withColumn("MinAnswersCreationDate", col("AnswersCreationDate").cast("timestamp"))\
    .groupBy(col("ParentId"))\
    .agg(min(col("AnswersCreationDate")).alias("Min_of_AnswersCreationDate"))
    
    Questions = Questions.join(Answers, Questions.Id == Answers.ParentId,how='inner')
    
    timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    timeDiff = (unix_timestamp('Min_of_AnswersCreationDate', format=timeFmt)\
            - unix_timestamp('CreationDate', format=timeFmt))
    Questions = Questions.withColumn('AnswerTime',timeDiff ) 
    Questions.show()
    return Questions

def CalculateActiveUser(Questoins,Answers,Users):
   
    Users = Users.select(col("Id").alias('UserId'),"Reputation","CreationDate","DisplayName")
    Answers=Answers.filter(col("OwnerUserId").isNotNull()).select("Id","ParentId","OwnerUserId",col("AnsweredYear"),col("AnsweredMonth"))
    
    #answerCount = Answers.count()
    
    Users_Answer_join = Answers.join(Users, Users.UserId == Answers.OwnerUserId,how='left')
    Users_Answer_join = Users_Answer_join.withColumnRenamed('Id','AnswerId')
    #Users_Answer_joinCount = Users_Answer_join.count()
    
    
    Questoins = Questoins.filter((col('AcceptedAnswerId').isNotNull() \
                                & col("Id").isNotNull())).select("Id","Tags","mappingResult","AcceptedAnswerId")
    
    Users_Answer_Questoins_join = Users_Answer_join.join(Questoins,Users_Answer_join.AnswerId==Questoins.AcceptedAnswerId,how='left') 
    
            
    Users_Answer_Questoins_join = Users_Answer_Questoins_join.groupBy("AnsweredYear","AnsweredMonth","Tags","UserId").count()\
            .withColumnRenamed("count","User_count_per_tag").filter(col('Tags').isNotNull()).sort(desc("User_count_per_tag"))
    
    #Users_Answer_Questoins_joinCount2 = Users_Answer_Questoins_join.count()
    
    Users_Answer_Questoins_join = Users_Answer_Questoins_join\
                               .withColumnRenamed("AnsweredYear","Year")\
                               .withColumnRenamed("AnsweredMonth","Month")
    
#     df=spark.createDataFrame([(answerCount,Users_Answer_joinCount,Users_Answer_Questoins_joinCount2)],[('answerCount','Users_Answer_joinCount','Users_Answer_Questoins_joinCount2')])
    #df.show()
    
    return Users_Answer_Questoins_join
    #return None
    