#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
TakeETFdata.py 
到Web抓取表格資料(read whole Table),儲存成csv
'''
import os, sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import datetime
import time

curPath = os.getcwd()
myHome = os.path.expanduser('~')
saveFolder = "ETFfile"

class hander:
    def __init__(self):
        self.starting = '0842ar122026'
            #options = webdriver.ChromeOptions()
            #options.add_argument("download.default_directory=saveFilePath")
            ##self.browser = webdriver.Chrome("/home/peihsuan/Downloads/chromedriver", chrome_options=options)
        self.driver = webdriver.Chrome("chromedriver")
        time.sleep(2)

    def entry(self):
    #Drop down: year/month/date/classfication
        self.driver.get('https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html')
        time.sleep(0.5)

        drop1 = Select(self.driver.find_element_by_name('yy'))
        drop1.select_by_value(yr)
        print("choose year:", yr)       

        drop2 = Select(self.driver.find_element_by_name('mm'))
        drop2.select_by_value(mh)
        print("choose month:", mh) 

        drop3 = Select(self.driver.find_element_by_name('dd'))
        drop3.select_by_value(dt)
        print("choose date:", dt) 

        drop4 = Select(self.driver.find_element_by_name('type'))
        drop4.select_by_value('0099P')
        print("choose type:", "ETF") 

        elem1 = self.driver.find_element_by_link_text('查詢')
        elem1.click()
        time.sleep(2)
        try:
            ###must give the xpath or won't find the element name
            self.driver.find_element_by_xpath("//*[@id='reports']")
            drop5 = Select(self.driver.find_element_by_name('report-table1_length'))
            drop5.select_by_value('-1')
            print("choose range:", "all") 
            print("web data ready!")
        except NoSuchElementException:
            elemA = self.driver.find_element_by_id("result-message")
            elemADeCode = elemA.get_attribute('innerHTML')
            Message = elemADeCode.encode('utf-8').strip()
            print(Message)
            #save csv 
            writeF = open(myHome+'/autoETFtry/'+saveFolder+'/'+fdatetime+'.csv', "w")
            writeF.write(Message+"\n")
            writeF.close()
            self.driver.close()
            sys.exit()
    def getDataAction(self):
        print(time.ctime())
        tableData = self.driver.find_element_by_id("report-table1")
        #print(tableData)
        try:
            dataCont = tableData.get_attribute('innerHTML')
            contEnDeCode = dataCont.encode('utf-8').strip()
            writeF = open(myHome+'/autoETFtry/'+saveFolder+'/'+fdatetime+'.csv', "w")
            soup = BeautifulSoup(contEnDeCode, "html.parser")
            #parse content
            for tr in soup.find_all("tr"):
                tmpStr = ""
                saveStr = ""
                idx = 0
                if len(tr.find_all("td")) == 0:
                    for ths in tr.find_all("th"):
                        tmpStr0 = ths.text
                        tmpStr0 = tmpStr0.replace("\r","")
                        tmpStr0 = tmpStr0.replace("\n","")
                        if idx == 0:
                            if len(tmpStr0) == 0:
                                tmpStr = "-"
                                saveStr = ""
                            else:
                                tmpStr = tmpStr0
                                saveStr = tmpStr0
                        else:
                            if len(tmpStr0) == 0:
                                tmpStr = tmpStr + "\t"+"-"
                                saveStr = saveStr +"@"+"-"
                            else:
                                tmpStr = tmpStr +"\t"+ tmpStr0
                                saveStr = saveStr +"@"+ tmpStr0
                        idx = idx+1
                    saveStr = saveStr.encode('utf-8').strip() #save by row
                    writeF.write(saveStr+"\n")
                else:
                    for tds in tr.find_all("td"):
                        tmpStr1 = tds.text
                        #print(type(tmpStr1.encode('utf-8').strip()),tmpStr1.encode('utf-8').strip())
                        tmpStr1 = tmpStr1.replace("\r","")
                        tmpStr1 = tmpStr1.replace("\n","")
                        if idx == 0: #first tds of every rows
                            if len(tmpStr1) == 0: #if tds is emptyData
                                tmpStr = "-"
                                saveStr = ""
                            else:
                                tmpStr = tmpStr1
                                saveStr = tmpStr1
                        else:
                            if len(tmpStr1) == 0:
                                tmpStr = tmpStr+ "\t"+ "-"
                                saveStr = saveStr+ "@"+ "-"
                            else:
                                tmpStr = tmpStr+ "\t"+ tmpStr1
                                saveStr = saveStr+ "@"+ tmpStr1
                        idx = idx+1
                    #tmpStr = tmpStr.encode('utf-8').strip()
                    saveStr = saveStr.encode('utf-8').strip() #save by row
                    writeF.write(saveStr+"\n")
                #idxj = 1
            #print('')
            writeF.close()
        except StaleElementReferenceException:
            pass
        self.driver.close()
#透過command line執行，其值會是'__main__'
#若由其他程式呼叫則不為'__main__'
if __name__ == '__main__':
    saveFilePath = myHome+'/autoETFtry/'+saveFolder #create the folder named"ETFfile"
    if not os.path.exists(saveFilePath):
        os.makedirs(saveFilePath)
        print('make dir')
    else: print('yes')
    print(curPath)
    curTime = datetime.datetime.now()
    fdatetime = curTime.strftime("%Y%m%d_%H%M")
    yr = str(curTime.year)
    mh = str(curTime.month)
    dt = str(curTime.day)
    h = hander()
    h.entry()
    h.getDataAction()
    print('All done')
