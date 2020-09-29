from pyspark.sql.functions import *
from pyspark.sql.types import *
from configFile import config
import random
import sys


def Preprocess(df_Posts,spark):
    Posts,Answers =SeperateAndDrop(df_Posts)
    TagSynMapping=TagDownload(spark)
    b_TagSynMapping = Broadcast(spark,TagSynMapping)
    Posts = MappingTag(b_TagSynMapping,Posts)
    return Posts,Answers


def SeperateAndDrop(df_Posts):
    
    columns_to_drop = ['LastEditorUserId','LastEditorDisplayName','Body']
    df_Posts = df_Posts.drop(*columns_to_drop)
    Posts,Answers = df_Posts.filter(df_Posts.PostTypeId == 1),df_Posts.filter(df_Posts.PostTypeId == 2)
    columns_to_drop_Posts,columns_to_drop_Answers =['PostTypeId','ParentId','CommentCount'], ['PostTypeId','AcceptedAnswerId','AnswerCount','FavoriteCount','Tags','Title','ViewCount']
    return Posts.drop(*columns_to_drop_Posts),Answers.drop(*columns_to_drop_Answers)
    

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
    
    TagSynMapping=SupplementTags2(spark,TagSynMapping,TagName)
    
    mapping  = {entry[0]:entry[1] for entry in TagSynMapping.collect()}
    return mapping
    """
    synonyms table
    +---------------+-----+                                                                                                                     |            key|value|                                                                                                                     +---------------+-----+                                                                                                                     |  windows-forms|    3|                                                                                                                     |       winforms|    3|
    +---------------+-----+
    """
    #TagName.show()
    #TagName = TagName.collect()
   
    #return SupplementTags(mapping,TagName)
   
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


def Broadcast(spark,mapping):
    return spark.sparkContext.broadcast(mapping)


def MappingTag(TagSynMapping,Posts):
    
    #Posts.rdd.map(udf(MapAndSort,StringType()))
    #print(TagSynMapping)
    return Posts.withColumn('mappingResult', working_fun(TagSynMapping)(col('Tags')))
      

def SupplementTags(TagSynMapping,TagName):
    
    #TagName
    Set = set()
    for val in TagSynMapping.values():
        Set.add(val)
    #print("TagMap 0 size",len(TagSynMapping))
    for tag in TagName:
        if tag not in TagSynMapping:
            rand = random.randint(0,sys.maxsize)
            while rand in Set:
                rand = random.randint(0,sys.maxsize)
            Set.add(rand)
            TagSynMapping[tag] = rand
    #print("TagMap 1 size",len(TagSynMapping))
    return TagSynMapping
    
def SupplementTags2(spark,TagSynMapping,TagName):
    TagSynMapping = TagSynMapping.select(col("key").alias("TagName"),col("value").alias("id"))
    TagName=TagName.drop('Count').withColumn("id", lit(None).cast(StringType())).select('*')
    
    result = TagSynMapping.union(TagName)
    from pyspark.sql import Window
    w = Window.partitionBy('TagName')
    result=result.select('TagName', 'id', count('TagName').over(w).alias('n'))

    result=result.dropDuplicates(subset=['TagName'])
    #result.show()
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
    result.show()
    return result