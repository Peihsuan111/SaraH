# -*- coding: utf-8 -*-
#alfred - friDay
'''
member info
'''
from datetime import datetime
def set_client(host_nm: str):
    from pymongo import MongoClient
    ci = {
        'raw_innet': (
                    '10.140.0.52:20001', 'eventuser', '1u03zj6u62j/4', 'events'),
        'raw_outnet': (
            '35.236.180.233:20001', 'eventuser', '1u03zj6u62j/4', 'events'),
        'alfred_outnet': (
            '35.194.200.228:27017', 'batuser', '1u03zj6u62j/4', 'alfred'),
        'alfred_innet': (
            '10.140.0.33:27017', 'batuser', '1u03zj6u62j/4', 'alfred'),
        'friDay_outnet':(
            '35.194.200.228:27017', 'batuser', '1u03zj6u62j/4', 'friDay'),
        'friDay_innet':(
            '10.140.0.33:27017', 'batuser', '1u03zj6u62j/4', 'friDay'),
        'meteor':(
            '35.201.252.6:20001', 'sara', '1u03zj6u62j/4', 'meteor'),
        'local_db': (
            'mongodb://localhost:27017/')
    }
    if host_nm == 'local_db':
        host = ci[host_nm]
        cli = MongoClient(host)
    elif host_nm in ci:
        host, unm, pwd, auth_db = ci[host_nm]
        cli = MongoClient(host=host, username=unm, password=pwd,
                    authSource=auth_db, authMechanism='SCRAM-SHA-1')
    else:
        return 'Hint: Wrong host name!'
    return (cli)


cli = set_client("friDay_innet")
coll = cli.friDay.mem_member_info
cur = coll.find({})


writeF = open('/home/sara/Files/friDay_member_Info.csv', "w", encoding='utf8')
colStr = 'memId@birth@zipData'
writeF.write(colStr+"\n")

for x in cur:
    #updTime = x["update_date"]
    #updTime = datetime.strptime(updTime, '%Y-%m-%d').date()
    #referenceDate = datetime.strptime('2019-12-31', '%Y-%m-%d').date()
    #if updTime > referenceDate:
    if ["ID"] not in x:
        pass 
    else:
        memId = x["ID"]
        birth = x["BIRTHDAY"]
        zipData = x["ZIP"]
        tmpStr = memId +"@"+ birth +"@"+ zipData
        saveStr = tmpStr.strip()
        try:
            writeF.write(saveStr+"\n")
        except ValueError:
            print(memId)
        else:
            pass
    
