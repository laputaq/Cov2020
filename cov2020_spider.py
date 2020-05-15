# -*- coding: utf-8 -*-
import requests
import pypyodbc
import json
import csv
import os
from pymysql import *

#连接MySQL
def connectMySQL():
    try:
        conn=connect(host='182.92.85.86',port=3306,user='root',password='3210',db='Cov2020')
        return conn
    except Exception as e:
        print(e)
    return NULL

#连接Access
def connectAccess():
    path=r"C:\Users\Qsh\Documents\Cov2020.accdb"
    
    try:
        conn = pypyodbc.connect(u'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + path)
        return conn
    except Exception as e:
        print(e)
    return NULL

#选择连接其中一个数据库
#db = connectMySQL()
db = connectAccess()

#检查数据库中表是否存在
def checkExists(tbName):
    #这里需要选择创建MySQL数据库还是Access数据库
    #b = createMySQL(tbName)
    b = createAccess(tbName)
    if b == False:
        print("表已存在")
        return False
    return True

#向MySQL中创建表
def createMySQL(tbName):
    cursor=db.cursor()
    try:
        #判断是否存在表
        res = cursor.execute("show tables like 'Cov%s'" % (tbName))
        #表已存在
        if res==1:
            return False
        
        cursor.execute("create table Cov%s(id int(11) primary key auto_increment, province varchar(64), city varchar(64), today_confirm int(11), exist_confirm int(11), total_confirm int(11), suspect int(11), dead int(11), heal int(11), dead_rate decimal(11, 2), heal_rate decimal(11, 2))" % (tbName))
        cursor.close()
        return True
    except Exception as e:
        print(e)
        db.rollback()
        cursor.close()
    return False

#向Access中创建表
def createAccess(tbName):
    cursor=db.cursor()
    try:
        cursor.execute("select * from Cov%s" % (tbName))
    except Exception:
        #表不存在
        cursor.execute("create table Cov%s(id counter primary key, province varchar(64), city varchar(64), today_confirm int, exist_confirm int, total_confirm int, suspect int, dead int, heal int, dead_rate float, heal_rate float)" % (tbName))
        cursor.close()
        return True
    return False

#向表中插入数据
def insertDB(tbName, fields):
    cursor = db.cursor()
    try:
        cursor.execute("insert into Cov%s(province, city, today_confirm, exist_confirm, total_confirm, suspect, dead, heal, dead_rate, heal_rate) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (tbName, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9]))
        db.commit()
        print("插入成功")
        cursor.close()
        return True
    except Exception as e:
        print(e)
        db.rollback()
        cursor.close()
    return False

#直接存储到数据库
def printDB(datas, tbName):
    if checkExists(tbName) ==False:
        return
    
    for info in datas:
        fields = [info.get("province"), info.get("city"), info.get("todayConfirm"), 
                  info.get("nowConfirm"),info.get("confirm"),info.get("suspect"), 
                  info.get("dead"), info.get("heal"), info.get("deadRate"), info.get("healRate")]
        insertDB(tbName, fields)
    
    db.close()

#转存到中间文件
def printCSV(datas, tbName):
    filePath = r'C:\Users\Qsh\Documents\Files\Cov2020\Cov' + tbName + '.csv'
    
    b = os.path.exists(filePath)
    if b == True:
        print("文件已存在")
        return
    
    fields = ["province", "city", "todayConfirm", "nowConfirm", "confirm", "suspect", "dead", 
              "heal", "deadRate", "healRate"]
    with open(filePath, 'a', encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(fields)
    
    for info in datas:
        fields = [info.get("province"), info.get("city"), info.get("todayConfirm"), 
                  info.get("nowConfirm"), info.get("confirm"), info.get("suspect"), 
                  info.get("dead"), info.get("heal"), info.get("deadRate"), info.get("healRate")]
        with open(filePath, 'a+', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(fields)
    
    #csv2db(filePath, tbName)

#中间文件存储到数据库
def csv2db(filePath, tbName):
    if checkExists(tbName) ==False:
        return
    
    with open(filePath, 'rt', encoding='utf-8') as f:
        reader = csv.reader(f)
        for fields in reader:
            insertDB(tbName, fields)
        

#爬取数据
def spider():
    url = 'https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5'
    json_text = requests.get(url).json()
    data = json.loads(json_text['data'])
    all_counties = data['areaTree']
    tbName = data['lastUpdateTime'][:10].replace("-", "_")
    
    datas = []
    for country_data in all_counties:
        if country_data['name'] == '中国':
            country_today = country_data['today']
            country_total = country_data['total']
            country_result = {'province': country_data['name'], 'city': country_data['name'], 
                              'todayConfirm': country_today['confirm']}
            country_result.update(country_total)
            datas.append(country_result)
            all_provinces = country_data['children']
            for province_data in all_provinces:
                province_name = province_data['name']
                all_cities = province_data['children']
                for city_data in all_cities:
                    city_name = city_data['name']
                    city_today = city_data['today']                    
                    city_total = city_data['total']
                    province_result = {'province': province_name, 'city': city_name, 
                                       'todayConfirm': city_today['confirm']}
                    province_result.update(city_total)
                    datas.append(province_result)
    
    printDB(datas, tbName)  #向数据库中输出数据
    #printCSV(datas, tbName)  #向中间文件输出数据

if __name__ == '__main__':
    spider()