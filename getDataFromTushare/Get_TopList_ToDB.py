'''
Created on 2021年7月18日

@author: SJLI

龙虎榜每日明细
接口：top_list
描述：龙虎榜每日交易明细
数据历史： 2005年至今
限量：单次最大10000
积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
trade_date	str	Y	交易日期
ts_code	str	N	股票代码
输出参数

名称	类型	默认显示	描述
trade_date	str	Y	交易日期
ts_code	str	Y	TS代码
name	str	Y	名称
close	float	Y	收盘价
pct_change	float	Y	涨跌幅
turnover_rate	float	Y	换手率
amount	float	Y	总成交额
l_sell	float	Y	龙虎榜卖出额
l_buy	float	Y	龙虎榜买入额
l_amount	float	Y	龙虎榜成交额
net_amount	float	Y	龙虎榜净买入额
net_rate	float	Y	龙虎榜净买额占比
amount_rate	float	Y	龙虎榜成交额占比
float_values	float	Y	当日流通市值
reason	str	Y	上榜理由

'''
import datetime
import time

import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL


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


def read_data():
    sql = """SELECT * FROM stock_basic LIMIT 20"""
    df = pd.read_sql_query(sql, engine)
    return df


def get_and_write_data(engine):
    pro = ts.pro_api()
    currentDate = datetime.datetime.now().strftime('%Y%m%d')
    # idate = '20050101'
    idate = currentDate
    itimes = 1;
    while idate <= currentDate:
        if itimes <= 80:
            df = pro.top_list(trade_date=idate)

            res = df.to_sql('hq_TopList', engine, index=False, if_exists='append', chunksize=10000,
                            dtype={'trade_date': DATE,
                                   'ts_code': NVARCHAR(20),
                                   'name': NVARCHAR(255),
                                   'close': DECIMAL(17, 2),
                                   'pct_change': DECIMAL(17, 6),
                                   'turnover_rate': DECIMAL(17, 6),
                                   'amount': DECIMAL(17, 2),
                                   'l_sell': DECIMAL(17, 2),
                                   'l_buy': DECIMAL(17, 2),
                                   'l_amount': DECIMAL(17, 2),
                                   'net_amount': DECIMAL(17, 2),
                                   'net_rate':  DECIMAL(17, 6),
                                   'amount_rate':  DECIMAL(17, 6),
                                   'float_values': DECIMAL(17, 2),
                                   'reason': NVARCHAR(255),})

            print(itimes, '   sql retrun:', res, '  date:', idate, '  df length:', len(df))
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
        else:
            itimes = 0
            time.sleep(61)
        itimes = itimes + 1
    return df


def droptable():
    engine.execute('drop table hq_TopList')


if __name__ == '__main__':
    # df = read_data()
    engine = init()
    # droptable()
    df = get_and_write_data(engine)
    print(df)
