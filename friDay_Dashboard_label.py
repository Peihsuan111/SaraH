# -*- coding: utf-8 -*-
#alfred - friDay
'''
batserver Environment
collection: 
    alfred.friday_order_tag
    alfred.friday_tag_family_id

debug: duplicate "tag_id"(collection: friday_order_tag)

'''
def current_time():
    import datetime
    curTime = datetime.datetime.now()
    curTime = curTime.strftime('%Y-%m-%d %H:%M:%S')
    return curTime

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

# create a family dictionary
def find_Name(family_tag_id):
    for F in lis_family:
        if F['tag_family_id'] == family_tag_id:
            return F['name']

def replacenth(string, sub, wanted, n):
    import re
    where = [m.start() for m in re.finditer(sub, string)][n-1]
    before = string[:where]
    after = string[where:]
    after = after.replace(sub, wanted, 1)
    newString = before + after
    return newString

def takeFirst(elem):
    return elem[0]


cli = set_client("alfred_innet")
coll_tag = cli.alfred.friday_tag_id
coll_family = cli.alfred.friday_tag_family_id
coll_order = cli.alfred.friday_order_tag
cur_tag = coll_tag.find({})
cur_order = coll_order.find({})

lis_family = list(coll_family.find({}))

# family name list-> column sequence list
family_name_list = []
for F in lis_family:
    family_name_list.append(F['name'])

# 做一個標籤對照表dict: label_dic -> could save as csv(wont update too often)
# label_dic[tag_id] = (tag_name, family_name) 
##lis = []
label_dic = dict()
for T in cur_tag:
    #dic = dict()
    key = T['tag_id']
    tag_name = T['name']
    family_name = find_Name(T['tag_family_id'])
    label_dic[key] = (tag_name,family_name)


# 做一個訂單名單結合對照表內容後一行一行寫入csv -> it worked
saveStr = ""
writeF = open('/home/sara/Files/friDay_label.csv', "w", encoding='utf-8')
colStr = 'orderId@' + str(family_name_list)[1:-1].replace(', ', '@')
writeF.write(colStr+"\n")
print('Start time:', current_time())

# need debug
'''
for i in cur_order:
    orderId = str(i['orderId'])
    fakeStr = '#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#'
    idx_lis = []
    for id in i['tag_id']:
        if id in label_dic:
            tag_nm = label_dic[id][0]
            family_nm = label_dic[id][1]
            idx = family_name_list.index(family_nm) + 1
            idx_lis.append((idx, tag_nm)) 
    minusCnt = 0
    idx_lis.sort(key=takeFirst) # sort by first value; lis = [(first, second), ...]
    for n in idx_lis:
        site = n[0] - minusCnt
        fakeStr = replacenth(fakeStr, '#', n[1] , site)
        minusCnt += 1
    saveStr = fakeStr.replace('#', '')
    saveStr = orderId + '@' + saveStr
    writeF.write(saveStr+"\n")'''

# debug fix:
for i in cur_order:
    orderId = str(i['orderId'])
    fakeStr = '#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#'
    idx_lis = []
    check_if_duplicate_tag_id = []
    for id in i['tag_id']:
        if id not in check_if_duplicate_tag_id:
            if id in label_dic:
                tag_nm = label_dic[id][0]
                family_nm = label_dic[id][1]
                idx = family_name_list.index(family_nm) + 1
                idx_lis.append((idx, tag_nm))
            else:
                pass
    minusCnt = 0
    idx_lis.sort(key=takeFirst) # sort by first value; lis = [(first, second), ...]
    for n in idx_lis:
        site = n[0] - minusCnt
        fakeStr = replacenth(fakeStr, '#', n[1] , site)
        minusCnt += 1
    saveStr = fakeStr.replace('#', '')
    saveStr = orderId + '@' + saveStr
    writeF.write(saveStr+"\n")

print('End time:', current_time())




'''===>take note
idx_lis.sort(key=takeSecond)



# 做一個訂單名單dataframe結合對照表內容: df -> dataframe work slowly

import pandas as pd
df = pd.DataFrame()
index_list = []
for i in cur_order:
    index_list.append(i['orderId'])
    dic = dict()
    for id in i['tag_id']:
        if id in label_dic:
            dic[label_dic[id][1]] = label_dic[id][0] #{family_name: tag_name}
    df = df.append(dic, True)
df.index = index_list


#test after append(dic) than write in csv ->doesnt work

## TypeError: write() argument must be str, not DataFrame
writeF = open('/home/sara/Files/labelTest.csv', "w", encoding='utf8')
data = []
dic = {'sex':'boy', 'resource':'google'}
dic2 = {'sex':'girl', 'year':'old_lady'}
testDF = pd.DataFrame()
writeTmp = testDF.append(dic, True)
#colStr = 'storeId@name@startDate@endDate@status@type@cart@saleStore'
writeF.write(writeTmp+"\n")

#test dic to df -> it worked

data = []
index_list = ['order1', 'order2']
dic = {'sex':'boy', 'resource':'google'}
dic2 = {'sex':'girl', 'year':'old_lady'}
#data.append(dic)
#data.append(dic2)
testDF = pd.DataFrame()
testDF = testDF.append(dic, True)
testDF = testDF.append(dic2, True)
testDF.index = index_list
'''
