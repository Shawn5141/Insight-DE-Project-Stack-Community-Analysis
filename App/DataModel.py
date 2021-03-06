import psycopg2
import pandas as pd
# Here you want to change your database, username & password according to your own values

import psycopg2
import sys
import os
sys.path.append(os.getcwd())
from configFile import *

conf = config()
param_dic = {
"host" : conf.host_ip,
"database" : conf.database,
"user" : conf.username,
"password" : conf.password
}


def generateQueryWithTag(Tags,yearRange):
    #print("generate Query",Tags,yearRange)
    BeginYear,EndYear =  yearRange[0],yearRange[1]
    tags = ["'"+word+"'" for word in Tags.split(',')]
    tags = ','.join(tags)

    return  f'SELECT "DisplayName" ,"UserId",SUM("User_count_per_tag")::float\
             FROM (SELECT *\
              FROM activeuserstable\
              WHERE "Year" BETWEEN {BeginYear} AND {EndYear} AND "Tags" = ARRAY[{tags}]::text[]) AS F\
             GROUP BY "DisplayName","UserId"\
             ORDER BY SUM("User_count_per_tag") DESC\
             limit 3'

def generateQueryWithUser(UserId,yearRange):
    #print("generate Query",Tags,yearRange)
    BeginYear,EndYear =  yearRange[0],yearRange[1]
#     tags = ["'"+word+"'" for word in Tags.split(',')]
#     tags = ','.join(tags)

    return  f'SELECT "Tags",SUM("User_count_per_tag")::float\
             FROM (SELECT *\
              FROM activeuserstable\
              WHERE "Year" BETWEEN {BeginYear} AND {EndYear} AND "UserId"={UserId} ) AS F\
             GROUP BY "Tags"\
             ORDER BY SUM("User_count_per_tag") DESC\
             limit 3'



def generateQueryForYearTag(Tags):
    tags = ["'"+word+"'" for word in Tags.split(',')]
    tags = ','.join(tags)

    return  f'SELECT "Year", "count"\
              FROM yeartagcount\
              WHERE "Tags" = ARRAY[{tags}]::text[]\
              ORDER BY "Year"'

def generateQueryForAnswersTime(Tags):
    tags = ["'"+word+"'" for word in Tags.split(',')]
    tags = ','.join(tags)

    return  f'SELECT "Year", "AnswerTimeSum"\
              FROM taganswertimecount\
              WHERE "Tags" = ARRAY[{tags}]::text[]\
              ORDER BY "Year"'

# def copy_from_file(conn, df, table):
#     """
#     Here we are going save the dataframe on disk as 
#     a csv file, load the csv file  
#     and use copy_from() to copy it to the table
#     """
#     # Save the dataframe to disk
#     tmp_df = "./tmp_dataframe.csv"
#     df.to_csv(tmp_df, index_label='id', header=False)
#     f = open(tmp_df, 'r')
#     cursor = conn.cursor()
#     try:
#         cursor.copy_from(f, table, sep=",")
#         conn.commit()
#     except (Exception, psycopg2.DatabaseError) as error:
#         os.remove(tmp_df)
#         print("Error: %s" % error)
#         conn.rollback()
#         cursor.close()
#         return 1
#     print("copy_from_file() done")
#     cursor.close()
#     os.remove(tmp_df)


def readDataFromPSQL(query):

    conn = connectWithDatabase(param_dic)
    cursor = conn.cursor()
    return getInsertData(cursor,query)
    
def connectWithDatabase(params_dic):
    try:
        conn = psycopg2.connect(**params_dic)

        #print("\n\n\n=============Connection to PostgreSQL created==========", "\n\n\n\n")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 

    return conn


# def createTable(conn,cmd):
#     cursor = conn.cursor()
#     try:
#         cursor.execute(cmd)

#         print("Created table in PostgreSQL", "\n")
#     except psycopg2.OperationalError as e:
#         print("================Something went wrong when creating the table==================", "\n")
#         print(e)

# def createIndex(cursor,IndexName,TableName,ColumnName):
#     try:
#         cursor.execute("CREATE INDEX {} ON {} ({})".format(IndexName,TableName,ColumnName))
                       
    
                       
#     except:
#         pass

def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def generateQuery(df):
    

    
    data = [tuple(x) for x in df.collect()]

    records_list_template = ','.join(['%s'] * len(data))

    insert_query = "INSERT INTO badges_table (Id,UserId,Name,Date,Class,TagBase \
                           ) VALUES {}".format(records_list_template)
    print("Inserting data into PostgreSQL...", "\n")
    return insert_query, data



# def insertToTable():
#     for i in dataframe.index:
#         query = """
#         INSERT into emissions(column1, column2, column3) values('%s',%s,%s);
#         """ % (dataframe['column1'], dataframe['column2'], dataframe['column3'])
#         single_insert(conn, query)


def getInsertData(cursor,query):

    cursor.execute(query)

    data = cursor.fetchall()
    return data
    
# def main():
#     config = config()
#     param_dic = {
#     "host" : config.host_ip,
#     "database" : config.database,
#     "user" : config.username,
#     "password" : config.password
# }
#     conn = connectWithDatabase(params_dic)
#     cmd = f'"CREATE TABLE IF NOT EXISTS {tableName} \
#        (Id SERIAL PRIMARY KEY, \
#          \
#         );"'
#     createTable(conn,cmd)
#     insertToTable()
#     conn.close()


    
