#!/usr/bin/env python
'''
Mac environment
friDay dashboard project
insert csv(& write in company name use file name) into bat Tables
'''
import psycopg2
import os,sys
import glob

def insert2table(filename, tablename):
    # connect to exist database:
    try:
        conn = psycopg2.connect("dbname='bat' user='huang' host='127.0.0.1' password=''")
    # or #conn = psycopg2.connect(host="localhost",database="suppliers", user="postgres", password="postgres")
    except:
        print("I am unable to connect to the database")
        sys.exit()
    curs = conn.cursor()

    # execute, Insert data in test"
    # read csv file
    fr = open(filename, 'r')
    dataList = fr.readlines()
    dataList = dataList[1:] #skip first row(column name)
    howManyColumn = len(dataList[0].split('@')) #count how many column ready be insert
    fr.close()
    cntRows = len(dataList)
    for i in range(cntRows):
        print(dataList[i])
        tmpStr = dataList[i]
        parseA = tmpStr.split('@')
        #insStr = "INSERT INTO friday_store VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (name,datetime) DO Update SET (open,high,low,close,adj_close,volume) = (EXCLUDED.open, EXCLUDED.high,EXCLUDED.low, EXCLUDED.close,EXCLUDED.adj_close,EXCLUDED.volume)"
        # create query string
        str = '%s'
        strs = repeatStr(str, howManyColumn)
        queryStr = " VALUES (" + strs + ")"
        insStr = "INSERT INTO " + tablename + queryStr
        data = tuple(parseA)
        cmdSql1 = curs.mogrify(insStr, data)
        curs.execute(cmdSql1)
        conn.commit()

    curs.close()
    conn.close()
    print( cntRows, 'data insert success' )


# insert manully
#insert_query =  "INSERT INTO TableName VALUES (%s, %s, %s, %s)", (10, 'hello@dataquest.io', 'Some Name', '123 Fake St.')
#cur.execute(insert_query)
#conn.commit()
# https://www.dataquest.io/blog/loading-data-into-postgres/
