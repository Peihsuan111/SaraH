# -*- coding: utf-8 -*-
#!/usr/bin/env python
'''
Mac environment
insert csv(& write in company name use file name) into Table
'''
import psycopg2
import os,sys
import glob
from db_insert import insert2table

#curDir = os.getcwd()
#print(curDir)
#resList = glob.glob('*.csv') #list of csv file name
#print(resList)

# insert friday_store data
'''
dataFile = '/Users/huang/Tableau_Project/friDay_store.csv'
tablename = 'friday_store'
insert2table(dataFile, tablename)
print('done storeInsert')
'''
# insert friday_label data
'''
import psycopg2
conn = psycopg2.connect("dbname='bat' user='huang' host='127.0.0.1' password=''")
curs = conn.cursor()
curs.execute("CREATE TABLE friday_label(orderId text PRIMARY KEY, 首購訂單 text, 金卡分類 text, 員工分類 text, 是否為hg會員 text, 遠傳資費方案會員 text, 是否接收簡訊 text, 是否接收edm text, 消費資訊 text, 訂單來源 text, 瀏覽來源 text, 居住地 text, 性別 text, 生日月份 text, 年齡 text, 遠傳門號會員 text, 訂單狀態 text, 下單裝置 text, 媒體投遞用 text, 分析用興趣分類 text, 分析用小類 text)")
conn.commit()
curs.close()
conn.close()
'''

insert2table('/Users/huang/Tableau_Project/friDay_label.csv', 'friday_label')


# create table:
#curs.execute("CREATE TABLE friday_store(storeId text PRIMARY KEY, name text, startDate text, endDate text, status text, type text, cart text, saleStore text)")
#conn.commit()
