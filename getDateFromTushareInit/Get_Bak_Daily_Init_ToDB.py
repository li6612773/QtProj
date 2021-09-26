'''
Created on 2021年7月18日

@author: SJLI

备用行情
接口：bak_daily
描述：获取备用行情，包括特定的行情指标
限量：单次最大5000行数据，可以根据日期参数循环获取，正式权限需要5000积分。

输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码
trade_date	str	N	交易日期
start_date	str	N	开始日期
end_date	str	N	结束日期
offset	str	N	开始行数
limit	str	N	最大行数
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	股票代码
trade_date	str	Y	交易日期
name	str	Y	股票名称
pct_change	float	Y	涨跌幅
close	float	Y	收盘价
change	float	Y	涨跌额
open	float	Y	开盘价
high	float	Y	最高价
low	float	Y	最低价
pre_close	float	Y	昨收价
vol_ratio	float	Y	量比
turn_over	float	Y	换手率
swing	float	Y	振幅
vol	float	Y	成交量
amount	float	Y	成交额
selling	float	Y	内盘（主动卖，手）
buying	float	Y	外盘（主动买， 手）
total_share	float	Y	总股本(万)
float_share	float	Y	流通股本(万)
pe	float	Y	市盈(动)
industry	str	Y	所属行业
area	str	Y	所属地域
float_mv	float	Y	流通市值
total_mv	float	Y	总市值
avg_price	float	Y	平均价
strength	float	Y	强弱度(%)
activity	float	Y	活跃度(%)
avg_turnover	float	Y	笔换手
attack	float	Y	攻击波(%)
interval_3	float	Y	近3月涨幅
interval_6	float	Y	近6月涨幅
'''
import datetime
import math
import time

import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL


def initEngineAndToken():
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


def initCodeList():
    sql = """select ts_code,list_date from hq_stock_basic order by ts_code"""
    codeList = pd.read_sql_query(sql, engine)
    return codeList


def get_data_and_toDB(codeList, engine):
    pro = ts.pro_api()
    currentDate = datetime.datetime.now().strftime('%Y%m%d')
    codeListArray = codeList.__array__()
    codes = ''
    i = 0
    itimes = 0
    icode = 0
    for code, list_date in codeListArray:
        # ipoDate = datetime.date.strftime(list_date, '%Y%m%d')
        codes = codes + ',' +code
        if i % 12 == 11 or i+1 == codeList.__len__():
            df = pro.bak_daily(ts_code=codes, start_date='20200101', end_date=currentDate)

            tosqlret = df.to_sql('hq_bak_daily', engine, chunksize=1000000, if_exists='append', index=False,
                                 dtype={'ts_code': NVARCHAR(20),
                                        'trade_date': DATE,
                                        'name': NVARCHAR(255),
                                        'pct_change': DECIMAL(17, 2),
                                        'close': DECIMAL(17, 2),
                                        'change': DECIMAL(17, 2),
                                        'open': DECIMAL(17, 2),
                                        'high': DECIMAL(17, 2),
                                        'low': DECIMAL(17, 2),
                                        'pre_close': DECIMAL(17, 2),
                                        'vol_radio': DECIMAL(17, 2),
                                        'turn_over': DECIMAL(17, 2),
                                        'swing': DECIMAL(17, 2),
                                        'vol': DECIMAL(17, 2),
                                        'amount': DECIMAL(17, 2),
                                        'selling': DECIMAL(17, 2),
                                        'buying': DECIMAL(17, 2),
                                        'total_share': DECIMAL(17, 2),
                                        'float_share': DECIMAL(17, 2),
                                        'pe': DECIMAL(17, 2),
                                        'industry': NVARCHAR(50),
                                        'area': NVARCHAR(50),
                                        'float_mv': DECIMAL(17, 2),
                                        'total_mv': DECIMAL(17, 2),
                                        'avg_price': DECIMAL(17, 2),
                                        'strength': DECIMAL(17, 2),
                                        'activity': DECIMAL(17, 2),
                                        'avg_turnover': DECIMAL(17, 2),
                                        'attack': DECIMAL(17, 2),
                                        'interval_3': DECIMAL(17, 2),
                                        'interval_6': DECIMAL(17, 2)})
            # print(tosqlret)
            print(math.ceil(i/12), '/', math.ceil(codeList.__len__()/12), codes, ' ', len(df))

            codes = ''
            itimes = itimes + 1
            if itimes == 10 :
                time.sleep(61)
                itimes = 0
        i = i + 1
    return df

# def droptable():
    # engine.execute('drop table hq_bak_daily')


if __name__ == '__main__':
    # df = read_data()
    engine = initEngineAndToken()  # 初始化sql引擎和Token数据
    codeList = initCodeList()  # 初始化证券列表
    print('证券列表数量：',codeList.__len__())
    df = get_data_and_toDB(codeList, engine)  # 读取行情数据，并存储到数据库
    print(codeList)
    # print(df)
    end_str = input("请符合是否正确执行！")
