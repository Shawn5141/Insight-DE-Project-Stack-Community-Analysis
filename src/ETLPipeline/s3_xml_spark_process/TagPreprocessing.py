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
    #TagSynMapping.show()
    mapping  = {entry[0]:entry[1] for entry in TagSynMapping.collect()}
    #TagName.show()
    TagName = TagName.collect()
    
    #
    return SupplementTags(mapping,TagName)
    """
    
    +---------------+-----+                                                                                                                     |            key|value|                                                                                                                     +---------------+-----+                                                                                                                     |  windows-forms|    3|                                                                                                                     |       winforms|    3|
    +---------------+-----+
    """
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
    