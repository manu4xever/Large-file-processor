import psycopg2
from psycopg2 import pool
#import multiprocessing as mp
import time
#import aggregate
import os

import csv
#import pandas as pd
#from multiprocessing.pool import ThreadPool as Pool
#from multiprocessing import Pool
#from random import randint
#import time
from contextlib import contextmanager



db_pool = pool.SimpleConnectionPool(1, 10,
                                    user = "postgres",
                                          password = "password",
                                          host = "127.0.0.1",
                                          port = "5432",
                                          database = "db_postman")



@contextmanager
def db():
    con = db_pool.getconn()
    cur = con.cursor()
    try:
        yield con, cur
    finally:
        cur.close()
        db_pool.putconn(con)


if __name__ == '__main__':
    tick=time.time()
    #os.system("python aggregate.py")
    filesource = 'products.csv'
    #aggregate(filesource)
    #df= pd.read_csv(filesource)
    
    #cursor = conn.cursor()
    with open(filesource, 'r') as f , db() as (connection, cursor):
        reader = csv.reader(f, delimiter=',')
        next(reader, None)
        for lines in reader:
    
        
            try:
                cursor.execute("INSERT INTO test_data " + \
            "(name, sku, description) " + \
            "VALUES (%s,%s, %s)"+\
            " ON CONFLICT (sku) DO UPDATE SET"+\
            "(name,description)=(EXCLUDED.name,EXCLUDED.description); ",(lines[0],lines[1],lines[2]))
                connection.commit()
                
            except psycopg2.Error as error:
                print('Database error:', error)
            except Exception as ex:
                print('General error:', ex)
        
    toc=time.time()
    print(toc-tick)
    
    