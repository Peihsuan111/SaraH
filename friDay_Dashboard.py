# -*- coding: utf-8 -*-
#alfred - friDay
'''
batserver Environment
'''

# print current time
def current_time():
    import datetime
    curTime = datetime.datetime.now()
    curTime = curTime.strftime('%Y-%M-%d %H:%m:%S')
    return curTime

# connect to db
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
coll = cli.friDay.order_detail

cur = coll.find({'ORDERMONTH':{'$in':[201901,201902,201903,201904,201905,201906,201907,201908,201909,201910,201911,201912, 202001,202002,202003,202004,202005,202006,202007,202008,202009,202010]}}
                ,{'ORDERID':1,'DEAL_ID':1,'ISO_YEARWEEK':1,'ORDERDAY':1,'ORDERTIME':1,
                'HOUSEID':1,'NEW_HOUSE_ID':1,'USERID':1,'YEARRABGE':1,'IS_FIRST_MONTH_BUY':1,'FINANCE_TYPE':1,'IS_FIRST_YEAR_BUY':1,'SEX':1,'DEVICE':1,'CHANNEL_ID3':1,
                'COUPON':1,'DISCOUNTCODE':1,'BENEFIT':1,'REBATE':1,'POINTS':1,'ITEM.SALES':1,'ITEM.COST_PURCHASE':1,'ITEM.PROFIT':1,'ITEM.NETPROFIT':1
                })
#-*- encoding:utf-8 -*-
saveStr = ""
writeF = open('/home/sara/Files/friDay_performance.csv', "w")
colStr = 'orderId@dealId@dateWeek@date@dateTime@storeId@new_storeId@memuid@yearRange@firstMonthBuy@financeType@firstYearBuy@sex@device@channelId3@sales@cost@profit@coupon@discountCode@fCoin@fCoinReturn@happyGo@netProfit'

writeF.write(colStr+"\n")
print('Start Time: ', current_time())
for x in cur:
    A = str(x['ORDERID'])
    B = str(x['DEAL_ID'])
    C = str(x['ISO_YEARWEEK'])
    D = str(x['ORDERDAY'])
    E = str(x['ORDERTIME'])
    F = str(x['HOUSEID'])
    G = str(x['NEW_HOUSE_ID'])
    H = str(x['USERID'])
    I = str(x['YEARRABGE'])
    J = str(x['IS_FIRST_MONTH_BUY'])
    K = str(x['FINANCE_TYPE'])
    L = str(x['IS_FIRST_YEAR_BUY'])
    M = str(x['SEX'])
    N = str(x['DEVICE'])
    O = str(x['CHANNEL_ID3'])
    sales = 0
    cost_purchases = 0
    profits = 0
    netprofits = 0
    # 加總訂單中的細項價格
    for i in x['ITEM']:
        sale = i['SALES']
        cost_purchase = i['COST_PURCHASE']
        profit = i['PROFIT']
        netprofit = i['NETPROFIT']
        sales = sales + sale
        cost_purchases = cost_purchases + cost_purchase
        profits = profits + profit
        netprofits = netprofits + netprofit
    P = str(sales) #P = str(x['SALES']) #1
    Q = str(cost_purchases) #Q = str(x['COST_PURCHASE']) #2
    R =  str(profits) #R = str(x['PROFIT']) #3
    S = str(x['COUPON'])
    T = str(x['DISCOUNTCODE'])
    U = str(x['BENEFIT'])
    V = str(x['REBATE'])
    W = str(x['POINTS'])
    X =  str(netprofits) #X = str(x['NETPROFIT']) #4
    tmpStr = A +"@"+ B +"@"+ C+"@"+D+"@"+E+"@"+F+"@"+G+"@"+H+"@"+I+"@"+J+"@"+K+"@"+L+"@"+M+"@"+N+"@"+O+"@"+P+"@"+Q+"@"+R+"@"+S+"@"+T+"@"+U+"@"+V+"@"+W+"@"+X
    saveStr = tmpStr.strip()
    writeF.write(saveStr+"\n")

print('End Time: ', current_time())
    #tmpStr = str(x['ORDERID'],encoding = 'utf-8') +"@"+ str(x['DEAL_ID'],encoding = 'utf-8') +"@"+ str(x['ISO_YEARWEEK'],encoding = 'utf-8')+"@"+ str(x['ORDERDAY'],encoding = 'utf-8')
