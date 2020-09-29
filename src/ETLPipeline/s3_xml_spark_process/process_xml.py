from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from xml_converter import convert_badges,convert_Posts
from configFile import config
from TagPreprocessing import Preprocess
import psycopg2
conf = config()

def initializeSpark():
    sc = SparkContext()
    #sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
    return SparkSession(sc)


def RunSpark(spark):
    df_Badges = convert_badges(spark,conf.s3file_Badges)
    df_Posts = convert_Posts(spark,conf.s3file_Posts)
    #Preprocessing the post and Map tag into tag id
    Posts,Answers = Preprocess(df_Posts,spark)
    #Posts.show()
    from pyspark.sql import Window
    #w = Window.partitionBy('mappingResult')
    dataFrame = Posts.select("Tags","mappingResult").groupBy("mappingResult","Tags").count().withColumnRenamed("count","Tag Number").sort(desc("count")).show(100, False)
    #Posts.select('Tags', count('mappingResult').over(w).alias('n')).sort(desc('n')).show(200,False)
    #Posts.select('Tags','mappingResult').groupBy('mappingResult').count().select(col('Tags'), col('count').alias('Tag Numbers')).show()
    #Posts.withColumn('Tag Numbers',Posts.groupBy('mappingResult').count()).select('Tags','Tag Numbers')
    
    
    #WriteToParquet(Posts,conf.s3file_parquet_Posts)
    #WriteToParquet(Answers,conf.s3file_parquet_Answers)
#     df_Posts.groupBy("Tags")
#     spark.sql('select Count(*) from post').show()
    #df_Posts.show()
    
    
    return {"Badges":df_Badges,'Posts':Posts,'Answers':Answers}



def StopSpark(spark):
    spark.stop()





    
def WriteToParquet(df,link):
    df.write.parquet(link)
    
if __name__=='__main__':
    spark = initializeSpark()
    RunSpark(spark)
    StopSpark(spark)
