# -*- coding: utf-8 -*-
#alfred - friDay
'''
*havent test success
//batserver Environment//
'''
## parameter
'''
from datetime import datetime
time_string = current_time()[:10]
defalt_month = datetime.strptime(time_string,'%Y-%m-%d').strftime('%Y%m')
input_month = []
input_month.append(int(defalt_month))
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

    # print current time
def current_time():
    import datetime
    curTime = datetime.datetime.now()
    curTime = curTime.strftime('%Y-%m-%d %H:%M:%S')
    return curTime

storeIdList = [0,1,2,3,4,7,8,11,158,663]
monthList = [201911,201912, 202001,202002,202003,202004,202005,202006,202007,202008,202009, 202010, 202011]

month = 201911

cli = set_client("friDay_innet")
coll = cli.friDay.order_detail
cur = coll.find({'ORDERMONTH':month})
saveStr = ""
writeF = open('/home/sara/Files/friDay_weather_perfromance.csv', "w", encoding='utf8')
colStr = 'orderid@dealId@orderDay@orderTime@houseId@sex@sales@cost_purchases@profits@name'
writeF.write(colStr+"\n")
print('Start Time: ', current_time())
findTrueLevel1_Id_List = []
for x in cur:
    storeId = x['NEW_HOUSE_ID'] #use this: for performance store
    if stortId in storeIdList: #only get specific store product
        orderId = x['ORDERID']
        dealId = x['DEAL_ID']
        orderDay = x['ORDERDAY']
        orderTime = x['ORDERTIME']
        houseId = x['HOUSEID']
        sex = x['SEX']
        sales = 0
        cost_purchases = 0
        profits = 0
        for i in x['ITEM']:
            if 'SALES' in i:
                if i['SALES'] is None:
                    sale = 0
                else:
                    sale = i['SALES']
            else:
                sale = 0
                print('No ITEM.SALES; orderId:', x['ORDERID'])

            if 'COST_PURCHASE' in i:
                if i['COST_PURCHASE'] is None:
                    cost_purchase = 0
                else:
                    cost_purchase = i['COST_PURCHASE']
            else:
                cost_purchase = 0
                print('No ITEM.COST_PURCHASE; orderId:', x['ORDERID'])
            if 'PROFIT' in i:
                if i['PROFIT'] is None:
                    profit = 0
                else:
                    profit = i['PROFIT']
            else:
                profit = 0
                print('No ITEM.PROFIT; orderId:', x['ORDERID'])
            sales = sales + sale
            cost_purchases = cost_purchases + cost_purchase
            profits = profits + profit
        sales = sales #P = str(x['SALES']) #1
        cost_purchases = cost_purchases #Q = str(x['COST_PURCHASE']) #2
        profits =  profits #R = str(x['PROFIT']) #3
        level1Id = x['LEVEL1_ID']
        if dic[level1Id][2] == 0: # chech sth->不是全部Level1ID parentID 都是0
            name = dic[level1Id][0] # map with prd_category->prd_category_name
            tmpStr = orderId +"@"+ dealId +"@"+ orderDay+"@"+orderTime+"@"+houseId+"@"+sex+"@"+sales+"@"+cost_purchases+"@"+profits+"@"+name
            saveStr = tmpStr.strip()
        else:
            findTrueLevel1_Id_List.append(orderId)
            print(orderId) #parentId -dic[level1Id][2]# 到category再查一次(dic)
    try:
        writeF.write(saveStr+"\n")
    except ValueError:
        print('error: ', x['ORDERID'])





    # connect to db

cli = set_client("friDay_innet")
coll = cli.friDay.prd_category
curFind = coll.find({})
dic = dict()
for x in curFind:
    if 'ID' in x:
        store = x["HOUSE_ID"]
        categoryNm = x["NAME"]
        parentId = x["PARENT_ID"]
        level1Id = x["ID"]
        dic[level1Id] = [categoryNm, store, parentId]
        else: pass
    else: pass 