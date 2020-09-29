
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import re
import html



def parse_line(line):
    pattern = re.compile(' ([A-Za-z]+)="([^"]*)"')
    return {key: value for key, value in pattern.findall(line)}


def convert_badges(spark, link):
    return spark.read.text(link).where(col('value').like('%<row Id%')) \
            .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
            .select(
                col('value.Id').cast('integer'),
                col('value.UserId').cast('integer'),
                col('value.Name'),
                col('value.Date').cast('timestamp'),
                col('value.Class').cast('integer'),
                col('value.TagBased').cast('boolean')
            )
   
parseTag = udf(lambda raw: html.unescape(raw).strip('>').strip('<').split('><') if raw else [], ArrayType(StringType()))
unescape = udf(lambda escape: html.unescape(escape) if escape else None)

def convert_Posts(spark,link):
    return spark.read.text(link).where(col('value').like('%<row Id%')) \
            .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value'))\
            .select(
            col('value.Id').cast('integer'),
            col('value.PostTypeId').cast('integer'),
            col('value.ParentId').cast('integer'),
            col('value.AcceptedAnswerId').cast('integer'),
            col('value.CreationDate').cast('timestamp'),
            col('value.Score').cast('integer'),
            col('value.ViewCount').cast('integer'),
            unescape('value.Body').alias('Body'),
            col('value.OwnerUserId').cast('integer'),
            col('value.LastEditorUserId').cast('integer'),
            col('value.LastEditorDisplayName'),
            col('value.LastEditDate').cast('timestamp'),
            col('value.LastActivityDate').cast('timestamp'),
            col('value.Title'),
            parseTag('value.Tags').alias('Tags'),
            col('value.AnswerCount').cast('integer'),
            col('value.CommentCount').cast('integer'),
            col('value.FavoriteCount').cast('integer')
    )
    