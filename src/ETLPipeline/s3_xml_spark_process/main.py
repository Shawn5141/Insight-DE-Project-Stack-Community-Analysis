from runSpark import initializeSpark,RunSpark,StopSpark 
from dataModel import connectWithDatabase, createTable, generateQuery,getInsertData,writeToJDBC
from configFile import config

def main():
    print("try to connect")
    #connection = connectWithDatabase(config())
    #cursor = connection.cursor()
    print("run spark")
    spark=initializeSpark()
    spark.sparkContext.setLogLevel("WARN")
    df = RunSpark(spark)
    
    #createTable(cursor)
    #query,data=generateQuery(df)
    #cursor.execute(query,data)
    #print("get data from database")
    #getInsertData(cursor)
    #writeToJDBC(df["Badges"],"badges",spark)
    StopSpark(spark)
    #cursor.close()
    #connection.commit()
    #connection.close()


if __name__=="__main__":
    main()
