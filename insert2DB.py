#!/usr/bin/env python
'''
insert csv(& write in company name use file name) into Table
'''
import psycopg2
import os,sys
import glob

curDir = os.getcwd()
print(curDir)
#resList = glob.glob('*.csv') #list of csv file name
#print(resList)
dataFile = '/media/psf/share/tmp/friDay_store.csv'

# connect to exist database:
try:
    conn = psycopg2.connect("dbname='postgres' user='huang' host='192.168.68.111' password=''")
# or #conn = psycopg2.connect(host="localhost",database="suppliers", user="postgres", password="postgres")
except:
    print("I am unable to connect to the database")
    sys.exit()

# open the cursor:
curs = conn.cursor()


# create TABLE
#curs.execute("CREATE TABLE Products(Id INTEGER PRIMARY KEY, Name VARCHAR(20), Price INT)")

# execute, Insert data in test"
# read csv file
fr = open(dataFile, 'r')
dataList = fr.readlines()
fr.close()

for i in range(len(dataList)):
    print(dataList[i])
    tmpStr = dataList[i]
    parseA = tmpStr.split('@')
    #insStr = "INSERT INTO friday_store VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (name,datetime) DO Update SET (open,high,low,close,adj_close,volume) = (EXCLUDED.open, EXCLUDED.high,EXCLUDED.low, EXCLUDED.close,EXCLUDED.adj_close,EXCLUDED.volume)"
    insStr = "INSERT INTO friDay_store VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    data = (parseA[0],parseA[1],parseA[2],parseA[3],parseA[4],parseA[5],parseA[6],parseA[7])    
    cmdSql1 = curs.mogrify(insStr, data)
    curs.execute(cmdSql1)
    conn.commit()
print(str(len(dataList))+' data insert success')
# insert manully
#insert_query =  "INSERT INTO TableName VALUES (%s, %s, %s, %s)", (10, 'hello@dataquest.io', 'Some Name', '123 Fake St.')
#cur.execute(insert_query)
#conn.commit()
# https://www.dataquest.io/blog/loading-data-into-postgres/

curs.close()
conn.close()
