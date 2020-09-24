from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from xml_converter import convert_badges


region='us-west-2'
bucket = 'stackoverflowdumpdata'
key = 'Badges.xml'





sc = SparkContext()
#sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)

s3file = f's3a://{bucket}/{key}'
text = convert_badges(spark,s3file)
text.show()


         
spark.stop()

