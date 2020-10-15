from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from configFile import config
import random
import sys


def Preprocess(spark,df_Posts):
    Posts,Answers =SeperateAndDrop(df_Posts)
    TagSynMapping=TagDownload(spark)
    b_TagSynMapping = Broadcast(spark,TagSynMapping)
    Posts = MappingTag(b_TagSynMapping,Posts)
    #Posts.select("Tags","mappingResult").show(30,truncate=False)
    return Posts,Answers


def SeperateAndDrop(df_Posts):
    
    columns_to_drop = ['LastEditorUserId','LastEditorDisplayName','Body','LastEditDate','LastActivityDate','Title']
    df_Posts = df_Posts.drop(*columns_to_drop)
    
    Question,Answers = df_Posts.filter(df_Posts.PostTypeId == 1).filter((col('AcceptedAnswerId').isNotNull())),\
                      df_Posts.filter(df_Posts.PostTypeId == 2)
    columns_to_drop_Question,columns_to_drop_Answers =['PostTypeId','ParentId','CommentCount','AnswerCount','FavoriteCount'], ['PostTypeId','AcceptedAnswerId','AnswerCount','FavoriteCount','Tags','Title','ViewCount']
    Question= Question.withColumn("Year",year(Question.CreationDate)).withColumn("Month",month(Question.CreationDate))
    
    Answers = Answers.withColumn("AnsweredYear",year(Question.CreationDate)).withColumn("AnsweredMonth",month(Question.CreationDate))
    
    return Question.drop(*columns_to_drop_Question),Answers.drop(*columns_to_drop_Answers)
    

def TagDownload(spark):
    conf = config()
    TagName=spark.read.option("header",True).csv(conf.TagNameLink)
    TagSyn=spark.read.option("header",True).csv(conf.TagSynLink)
    TagSyn.drop(*['CreationDate','TargetTagName','OwnerUserId','AutoRenameCount'\
                  ,'LastAutoRename','Score','ApprovedByUserId','ApprovalDate'])
    
    TagSyn = TagSyn.withColumn("MAPPING",create_map(
       'SourceTagName',
        'Id',
        'TargetTagName',
        'Id'))
    TagSynMapping = TagSyn.select(explode(TagSyn.MAPPING))
    """
    synonyms table
    +---------------+-----+                                                                                                                     |            key|value|                                                                                                                     +---------------+-----+                                                                                                                     |  windows-forms|    3|                                                                                                                     |       winforms|    3|
    +---------------+-----+
    """
    TagSynMapping=SupplementTags2(spark,TagSynMapping,TagName) 
    return {entry[0]:entry[1] for entry in TagSynMapping.collect()}
    
    
def Broadcast(spark,mapping):
    return spark.sparkContext.broadcast(mapping)
    
def working_fun(mapping_broadcasted):
    def f(x):
        tmp = []
        for word in x:
            if word and mapping_broadcasted.value.get(word):
                tmp+=[mapping_broadcasted.value.get(word)]
        if tmp==[]:
            return ''
        tmp.sort()
        return ','.join(tmp)
       
    return udf(f)


def MappingTag(TagSynMapping,Posts):
    return Posts.withColumn('mappingResult', working_fun(TagSynMapping)(col('Tags')))
      

    
def SupplementTags2(spark,TagSynMapping,TagName):
    # http://ec2-44-235-91-5.us-west-2.compute.amazonaws.com:8888/notebooks/findspark.py.ipynb
    TagSynMapping = TagSynMapping.select(col("key").alias("TagName"),col("value").alias("id"))
    TagName=TagName.drop('Count').withColumn("id", lit(None).cast(StringType())).select('*')
    # Concate two column
    result = TagSynMapping.union(TagName)
    w = Window.partitionBy('TagName')
    result=result.select('TagName', 'id', count('TagName').over(w).alias('n'))
    # Get rid of duplicate
    result=result.dropDuplicates(subset=['TagName'])
    # Generate mapping from name to 
    mapping=result.groupBy(result.id).count().select('id', col('count').alias('n')).withColumn("id2", monotonically_increasing_id()).select('*')
    #mapping.show()
    def working_fun2(mapping):
        def f(x):
            return mapping.value.get(x)

        return udf(f)
    mapping = {entry[0]:entry[2] for entry in mapping.collect()}
    
    b = spark.sparkContext.broadcast(mapping)
    result=result.withColumn('new_id',when(col('id').isNotNull()  ,working_fun2(b)(col('id'))).otherwise(monotonically_increasing_id()))
    result=result.drop('id','n')
    #result.show()
    return result