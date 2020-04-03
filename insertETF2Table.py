#!/usr/bin/env python
'''
insert ETF csvFiles into DB
'''
import psycopg2
import os,sys
import glob
import re
import shutil

curDir = os.getcwd()
print(curDir)
fileDir = "ETFfile"
resList = glob.glob(fileDir +"/"+ '*.csv') #list of csv file name
print(resList)

# connect to exist database:
try:
    conn = psycopg2.connect("dbname='TRY' user='custom' host='localhost' password='custom'")
except:
    print("I am unable to connect to the database")
    sys.exit()

# open the cursor:
curs = conn.cursor()

for j in range(len(resList)):
    fileName = resList[j] #ETFfile/file's name
    fileSplit = fileName.split(".", 2)
    parseN = fileSplit[0]
    fileDate = re.search('[0-9]+', fileSplit[0]).group(0)
    dataFile = curDir+"/"+fileName
    print(dataFile)
    # create TABLE
    #curs.execute("CREATE TABLE Products(Id INTEGER PRIMARY KEY, Name VARCHAR(20), Price INT)")

    # execute, Insert data in test"
    # read csv file
    fr = open(dataFile, 'r')
    dataList = fr.readlines()
    fr.close()
    print(parseN)
    succCount = 0

    for i in range(2,len(dataList),1):
        #print(parseN)
        #print(dataList[i])
        tmpStr = dataList[i]
        parseA = tmpStr.split('@')
        if len(parseA) == 16:
            insStr = "INSERT INTO etf VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (name,datetime) DO Update SET (vol_share, vol_num, vol_amount, open, high, low, close, spread, lastBuy_amount, lastBuy_vol, lastSell_amount, lastSell_vol, peratio) = (EXCLUDED.vol_share, EXCLUDED.vol_num, EXCLUDED.vol_amount, EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.spread, EXCLUDED.lastBuy_amount, EXCLUDED.lastBuy_vol, EXCLUDED.lastSell_amount, EXCLUDED.lastSell_vol, EXCLUDED.peratio)"
            data = (parseA[0],fileDate,parseA[2],parseA[3],parseA[4],parseA[5],parseA[6],parseA[7],parseA[8],parseA[9],parseA[10],parseA[11],parseA[12],parseA[13],parseA[14])    
            try:
                cmdSql1 = curs.mogrify(insStr, data)
                curs.execute(cmdSql1)
                conn.commit()
                print ("insert record success")
                succCount += 1
                
                
                
            except psycopg2.DatabaseError, e:
                print("Fail to insert")
                if conn:
                    conn.rollback() #kill temp store data,and error print out
                    print ('Error %s by insert', e)
        else:
            print("data unmatch!")
            pass
        #print(str(len(dataList))+' data insert success')
        if succCount == len(dataList)-2:
            # mv csv to folder:
            yr = fileDate[:4]
            mt = fileDate[4:6]
            mvfile = curDir +"/"+ fileName
            dstDir = curDir +"/"+ yr +"/"+ mt
            if not os.path.exists(dstDir):
                os.makedirs(dstDir)
                print('make dir')
            else: 
                pass
            shutil.move(mvfile, dstDir)
            print("mv file success")

    # psycopg2
    # https://www.dataquest.io/blog/loading-data-into-postgres/
curs.close()
conn.close()
