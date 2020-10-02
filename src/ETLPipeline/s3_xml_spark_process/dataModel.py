

import psycopg2
from configFile import config
from pyspark.sql.types import (
    ShortType,
    StringType,
    StructType,
    StructField,
    TimestampType,
    FloatType
)



config = config()
def connectWithDatabase(config):
    try:
        conn = psycopg2.connect(
          host = config.host_ip,
          database = config.database,
          user = config.username,
          password = config.password)

        print("\n\n\n=============Connection to PostgreSQL created==========", "\n\n\n\n")
    except:
        print("\n\n\n++++++++can not connect=========================\n\n\n\n")
	

    return conn


def createTable(cursor):

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS badges_table \
       (Id SERIAL PRIMARY KEY, \
        UserId INTEGER  NOT NULL, \
        Name VARCHAR(255) NOT NULL, \
        Date DATE , \
        Class INTEGER, \
        TagBase BOOLEAN \
        );")

        print("Created table in PostgreSQL", "\n")
    except psycopg2.OperationalError as e:
        print("================Something went wrong when creating the table==================", "\n")
        print(e)

# def createIndex(cursor,IndexName,TableName,ColumnName):
#     try:
#         cursor.execute("CREATE INDEX {} ON {} ({})".format(IndexName,TableName,ColumnName))
                       
    
                       
#     except:
#         pass
def generateQuery(df):
    

    
    data = [tuple(x) for x in df.collect()]

    records_list_template = ','.join(['%s'] * len(data))

    insert_query = "INSERT INTO badges_table (Id,UserId,Name,Date,Class,TagBase \
                           ) VALUES {}".format(records_list_template)
    print("Inserting data into PostgreSQL...", "\n")
    return insert_query, data

def writeToJDBC(df,tableName,spark):
    #df.table(tableName).write.jdbc(config.jdbcUrl,tableName,config.connectionProperties)
    #df = df.na.fill(0)
    
    """
    field = [
    StructField("MULTIPLIER", FloatType(), True),
    StructField("DESCRIPTION", StringType(), True),
    ]
    schema = StructType(field)
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    df.show()

    """
    mode= "overwrite"
    #print("jdbcURL: ",config.jdbcUrl,"\ntable Name :",tableName,"\nmode:",mode,"\nconnection property",config.connectionProperties,"\n")
    try:
      
      df.write.jdbc(url=config.jdbcUrl, table=tableName, mode=mode, properties=config.connectionProperties)
      print("Inserting data into PostgreSQL...", "\n")
    except Exception as e:
      print(e)






def getInsertData(cursor):

    postgreSQL_select_Query = "select Id,UserId ,Class  from badges_table"

    cursor.execute(postgreSQL_select_Query)

    cars_records = cursor.fetchmany(2)

    #print("Printing 2 rows")
    for row in cars_records:
        print("Id = ", row[0], )
        print("UserId = ", row[1])
        print("Class  = ", row[2], "\n")



