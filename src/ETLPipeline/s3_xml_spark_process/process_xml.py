from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from xml_converter import convert_badges,convert_Posts
from configFile import config
import psycopg2
conf = config()

def initializeSpark():
    sc = SparkContext()
    #sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
    return SparkSession(sc)
def RunSpark(spark):
    df_Badges = convert_badges(spark,conf.s3file_Badges)
    
    df_Posts = convert_Posts(spark,conf.s3file_Posts)
    #df_Posts.createOrReplaceTempView("post");
    #Preprocessing the post
    Posts,Answers =SeperateAndDrop(df_Posts)
    Posts.select('Tags').show()
    Answers.show()
    
    #WriteToParquet(Posts,conf.s3file_parquet_Posts)
    #WriteToParquet(Answers,conf.s3file_parquet_Answers)
#     df_Posts.groupBy("Tags")
#     spark.sql('select Count(*) from post').show()
    #df_Posts.show()
    
    
    return {"Badges":df_Badges,'Posts':Posts,'Answers':Answers}

def StopSpark(spark):
    spark.stop()

def SeperateAndDrop(df_Posts):
    
    columns_to_drop = ['LastEditorUserId','LastEditorDisplayName','Body']
    df_Posts = df_Posts.drop(*columns_to_drop)
    Posts,Answers = df_Posts.filter(df_Posts.PostTypeId == 1),df_Posts.filter(df_Posts.PostTypeId == 2)
    columns_to_drop_Posts,columns_to_drop_Answers =['PostTypeId','ParentId','CommentCount'], ['PostTypeId','AcceptedAnswerId','AnswerCount','FavoriteCount','Tags','Title','ViewCount']
    return Posts.drop(*columns_to_drop_Posts),Answers.drop(*columns_to_drop_Answers)
    
def TagDownload():
    TagName=spark.read.text(conf.TagNameLink)
    TagSyn=spark.read.text(conf.TagSyn)
#         s3 = boto3.resource('s3')
#         s3.meta.client.download_file(self.bucketTag, 'spark-warehouse'+self.TagName,self.TagName)
#         s3.meta.client.download_file(self.bucketTag, 'spark-warehouse'+self.TagSync,self.TagSync)
    

def WriteToParquet(df,link):
    df.write.parquet(link)
    
if __name__=='__main__':
    spark = initializeSpark()
    RunSpark(spark)
    StopSpark(spark)
