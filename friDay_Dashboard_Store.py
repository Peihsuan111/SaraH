# -*- coding: utf-8 -*-
#alfred - friDay
'''
insert store info to ...

//batserver Environment//
update frequency: 3month
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

cli = set_client("friDay_innet")
coll = cli.friDay.str_product_store
cur = coll.find({'STOREID':{'$exists':True}})

saveStr = ""
writeF = open('/home/sara/Files/friDay_store.csv', "w", encoding='utf-8')
colStr = 'storeId@name@startDate@endDate@status@type@cart@saleStore'

writeF.write(colStr+"\n")
for x in cur:
    A = str(x['STOREID'])
    B = str(x['NAME'])#B = str(x['NAME'].encode('utf-8'))
    C = str(x['STARTDATE'])
    D = str(x['ENDDATE'])
    E = str(x['STATUS'])
    F = str(x['TYPE'])
    G = str(x['CART'])
    H = str(x['SALE_STORE'])
    tmpStr = A +"@"+ B +"@"+ C+"@"+D+"@"+E+"@"+F+"@"+G+"@"+H
    saveStr = tmpStr.strip()
    try:
        writeF.write(saveStr+"\n")
    except ValueError:
        print(x['STOREID'])
