import pandas as pd
from sqlalchemy import create_engine
import sys
import os
sys.path.append(os.getcwd())
from configFile import config
conf = config()

# follows django database settings format, replace with your own settings

# construct an engine connection string
engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
    user = conf.username,
    password = conf.password,
    host = conf.host_ip,
    port = conf.port,
    database =conf.database,
)

# create sqlalchemy engine
engine = create_engine(engine_string)

# read a table from database into pandas dataframe, replace "tablename" with your table name
df = pd.read_sql_table('activeuserstable',engine)
print(df.head())

def connectWithDatabase(params_dic):
    try:
        conn = psycopg2.connect(**params_dic)

        print("\n\n\n=============Connection to PostgreSQL created==========", "\n\n\n\n")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 

    return conn
def getInsertData(cursor,query):

    #postgreSQL_select_Query = "select Id,UserId ,Class  from badges_table"
    
    cursor.execute(query)

    data = cursor.fetchall()

#     #print("Printing 2 rows")
#      #Year |              Tags              |  UserId  |   DisplayName    | User_count_per_tag 
#     for row in cars_records:
#         print("Year = ", row[0], )
#         print("Tags = ", row[1])
#         print("UserId = ", row[2])
#         print("DisplayName  = ", row[3])
#         print("DisplayName  = ", row[4], "\n")
    return data
def generateQueryWithTag(Tags,beginYear,endYear):
    tags = ["'"+word+"'" for word in Tags.split(',')]
    tags = ','.join(tags)

    return  f'SELECT "DisplayName" ,"UserId",SUM("User_count_per_tag")\
             FROM (SELECT *\
              FROM activeuserstable\
              WHERE "Year" BETWEEN {BeginYear} AND {EndYear} AND "Tags" = ARRAY[{tags}]::text[]) AS F\
             GROUP BY "DisplayName","UserId"\
             ORDER BY SUM("User_count_per_tag") DESC\
             limit 10'

def readDataFromPSQL(query):
#     conf = config()
#     print(conf.password)
#     connection = pg.connect(f'host={conf.host_ip}, dbname={conf.database}, user={conf.username}, password={conf.password}')
#     dataframe = psql.read_sql(cmd, connection)
#     print(dataframe.head())
    
    
    conn = connectWithDatabase(param_dic)
    cursor = conn.cursor()
    return getInsertData(cursor,query)