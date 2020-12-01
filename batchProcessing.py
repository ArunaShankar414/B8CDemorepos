import os
import psycopg2
import pandas as pd
import numpy as np
from psycopg2 import errors
 #Specification of Connection
conn_parameter= {
    "host"   : "localhost",
    "database" : "Demodb",
    "user"   : "Aruna",
    "password" : "aruna"
}
   #Load  data from  the file
csv_file = "/home/kit/Documents/pandas2postgresql/data/global-temp-monthly.csv"
dataframe=pd.read_csv(csv_file)
print(dataframe,"This the output of Dataframe")
dataframe = dataframe.rename(columns={
            "Source": "source",
             "Date": "datetime",
              "Mean": "mean_temp"
    })
print(dataframe)
    #Connect to the  postgresql server
def connect(conn_parameter):
        conn = None
        try:
            print("Connecting to Postgresql server")
            conn=psycopg2.connect(**conn_parameter)
            print(conn)
        except (Exception,psycopg2.DatabaseError)as error:
            print(error)
            #sys.exit(1)
        print("Connection Successful")
        return conn
conn=connect(conn_parameter)
print(conn)
    #batch Insert or Bulk Update
def execute_Myfile(conn, dataframe, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tuples from the dataframe values
    tuples = [tuple(x) for x in dataframe.to_numpy()]
    print(tuples,"This is the output of tuples")
    # Comma-separated dataframe columns
    cols = ','.join(list(dataframe.columns))
    print("Its Meeeeeeeeeeee",cols)
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES (%%s,%%s,%%s)" % (table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()
# Run the execute_many strategy
execute_Myfile(conn, dataframe, 'Monthlytemp')