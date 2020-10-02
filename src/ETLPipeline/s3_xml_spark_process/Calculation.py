from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

def CalculateYearTagCount(Questions):
    df = Questions.select("Tags","mappingResult","Year","Month").groupBy("mappingResult","Tags","Year","Month").count()\
    .withColumnRenamed("count","Tag in each Year/Month")
    df.show()
    YearTagCount = df.groupBy(df.Tags,df.Year,df.Month).agg({"Tag in each Year/Month": "sum"}).alias('Tag/Time Number Sum').sort(desc("sum(Tag in each Year/Month)"))
    YearTagCount.show()
    return YearTagCount

def CalculateMutualTagCount(Questions):
    MutualTagCount = Questions.select("Tags","mappingResult").groupBy("mappingResult","Tags").count().withColumnRenamed("count","Tag Number").sort(desc("count"))
    return MutualTagCount

def CalculateSingleTagCount(Questions):
    MutualTagCount = Questions.select("Tags","mappingResult").groupBy("mappingResult","Tags").count().withColumnRenamed("count","Tag Number").sort(desc("count"))
    df = MutualTagCount.select(explode(MutualTagCount.Tags).alias('Tags'),col("Tag Number"))
    SingleTagCount=df.groupBy(df.Tags).agg({"Tag Number": "sum"}).alias('Tag Number Sum').sort(desc("sum(Tag Number)"))
    return SingleTagCount

    
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
    timeDiff = (unix_timestamp('CreationDate', format=timeFmt)\
            - unix_timestamp('Min_of_AnswersCreationDate', format=timeFmt))
    Questions.withColumn('AnswerTime',timeDiff ) 
    Questions.show()
    return Questions

def CalculateActiveUser(Questoins,Answers,Users):
   
    Users = Users.select(col("Id").alias('UserId'),"Reputation","CreationDate","DisplayName")
    Answers=Answers.select("ParentId","OwnerUserId","Year","Month")
    #Users = Users.join(Answers, Users.Id == Answers.OwnerUserId,how='inner')
    Users_Answer_join = Answers.join(Users, Users.UserId == Answers.OwnerUserId,how='left')
    Questoins = Questoins.select("Id","Tags","mappingResult")
    Users_Answer_Questoins_join = Users_Answer_join.join(Questoins,Users_Answer_join.ParentId == Questoins.Id,how='left')
    Users_Answer_Questoins_join.show()
            
    Users_Answer_Questoins_join = Users_Answer_Questoins_join.groupBy("Tags","UserId").count()\
            .withColumnRenamed("count","User_count_per_tag").sort(desc("User_count_per_tag"))
    Users_Answer_Questoins_join.show()
    return Users_Answer_Questoins_join
    