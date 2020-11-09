# -*- coding: utf-8 -*-
#alfred - friDay
'''
batserver Environment
'''
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

def current_time():
    import datetime
    curTime = datetime.datetime.now()
    curTime = curTime.strftime('%Y-%m-%d %H:%M:%S')
    return curTime

cli = set_client("friDay_innet")
coll = cli.friDay.channel_report
cur = coll.find({'ORDERID':{'$exists': True}})

saveStr = ""
writeF = open('/home/sara/Files/friDay_bonus.csv', "w", encoding='utf-8')
colStr = 'orderId@update_date@BONUS@CHANNELID'

print('Start Time: ', current_time())
writeF.write(colStr+"\n")
for x in cur:
    A = str(x['ORDERID'])
    B = str(x['update_date'])#B = str(x['NAME'].encode('utf-8'))
    C = str(x['BONUS'])
    D = str(x['CHANNELID'])
    tmpStr = A +"@"+ B +"@"+ C+"@"+D
    saveStr = tmpStr.strip()
    try:
        writeF.write(saveStr+"\n")
    except ValueError:
        print(x['ORDERID'])

 print('End Time: ', current_time())
