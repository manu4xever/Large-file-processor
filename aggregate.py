# -*- coding: utf-8 -*-
"""
Created on Sun Jun 28 19:40:18 2020

@author: manui
"""
import psycopg2
import sys
import pandas as pd
import time


param_dic = {
    "host"      : "127.0.0.1",
    "database"  : "db_postman",
    "user"      : "postgres",
    "password"  : "password",
    "port"      : "5432"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn


def copy_from_file(conn, df, table):
    """
    Here we are going save the dataframe on disk as 
    a csv file, load the csv file  
    and use copy_from() to copy it to the table
     """
    #delete the previous entries on table
    
    cursor = conn.cursor()
   
    cursor.execute("Delete from agg ;")
    conn.commit()
    
    # Save the dataframe to disk
    
    tmp_df = "./tmp_dataframe.csv"
    df.to_csv(tmp_df, header=False,index=False)
    f = open(tmp_df, 'r')
    
    try:
        cursor.copy_from(f, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        #os.remove(tmp_df)
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_file() done")
    cursor.close()
    
    #os.remove(tmp_df)
def aggregate(file):
    df= pd.read_csv(filesource)
    agg=df.groupby(['name'])['sku'].count()
    agg = agg.to_frame()
    agg.columns = [ 'no.of products']
    agg.reset_index()
    agg.reset_index(level=0, inplace=True)
    return agg
    #print(type(agg))
    #agg.to_frame
    #print(type(agg))
    #new=pd.DataFrame(agg.values,columns=['name', 'no.of products'])
    #print(agg.head(100))

if __name__ == '__main__':
    tic=time.time()
    filesource = 'products.csv'
    agg=aggregate(filesource)
    conn = connect(param_dic)
    res=copy_from_file(conn,agg,'agg')
    toc=time.time()
    print(toc-tic)
  