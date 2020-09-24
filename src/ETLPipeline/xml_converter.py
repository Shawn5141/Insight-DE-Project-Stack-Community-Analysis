
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
    
