'''
Created on 2021年8月15日

@author: SJLI

券商每月荐股
接口：broker_recommend
描述：每月初获取券商月度金股
限量：单次最大1000，积分达到600即可调用，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
month	str	Y	月度（YYYYMM）
输出参数

名称	类型	默认显示	描述
month	str	Y	月度
broker	str	Y	券商
ts_code	str	Y	股票代码
name	str	Y	股票简称
接口示例


#获取查询月份券商金股
df = pro.broker_recommend(month='202106')
数据示例

             month broker    ts_code  name
0    202106   东兴证券  000066.SZ  中国长城
1    202106   东兴证券  000708.SZ  中信特钢
2    202106   东兴证券  002304.SZ  洋河股份
3    202106   东兴证券  003816.SZ  中国广核
4    202106   东兴证券  300196.SZ  长海股份
..      ...    ...        ...   ...
263  202106   长城证券  600096.SH   云天化
264  202106   长城证券  600809.SH  山西汾酒
265  202106   长城证券  603596.SH   伯特利
266  202106   长城证券  603885.SH  吉祥航空
267  202106   长城证券  605068.SH  明新旭腾
'''
import datetime
import time

import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR,DATE,Integer,DECIMAL


def init():
    # 初始化pro接口
    token = '416ae9fd9b5e3c1180827bd24f729982057f67cbfd75af9df7fe9a87'
    print(ts.__version__)
    # 设置token
    ts.set_token(token)
    # 初始化数据库
    connect_info = 'mysql+pymysql://root:12345@localhost:3306/qtdb' \
                   ''
    engine = create_engine(connect_info)  # use sqlalchemy to build link-engine
    print(engine, '数据库链接初始化成功')
    return engine


# def read_data():
#     sql = """SELECT * FROM stock_basic LIMIT 20"""
#     df = pd.read_sql_query(sql, engine)
#     return df


def write_data(df,engine):
    res = df.to_sql('hq_broker_recommend', engine, index=False, if_exists='append', chunksize=10000,
                    dtype={'month': NVARCHAR(6),
                           'broker': NVARCHAR(255),
                           'ts_code': NVARCHAR(20),
                           'name': NVARCHAR(255)})
    print(res)


def get_data():
    currentDate = datetime.datetime.now().strftime('%Y%m')
    idate = '200001'
    nextdate = '199912'
    nextdateP = datetime.datetime.strptime('2000-01-01','%Y-%m-%d').date()
    # nextdate = currentDate
    # nextdateP = datetime.datetime.now()
    itimes = 1;
    while nextdate <= currentDate:
        if itimes <= 5:
            if idate != nextdate:
                pro = ts.pro_api()
                df = pro.broker_recommend(month=nextdate)
                write_data(df, engine)
                print(nextdate)
                print(df)
                idate = nextdate
                itimes = itimes + 1
            nextdateP = nextdateP + datetime.timedelta(days=20)
            nextdate = nextdateP.strftime('%Y%m')

        else:
            itimes = 0
            time.sleep(61)
            itimes = itimes + 1


def droptable():
    engine.execute('drop table if exists  hq_broker_recommend')

if __name__ == '__main__':
    # df = read_data()
    engine = init()
    droptable()
    get_data()
    end_str = input("请符合是否正确执行！")
