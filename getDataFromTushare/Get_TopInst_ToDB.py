'''
Created on 2021年7月20日

@author: SJLI

龙虎榜机构明细
接口：top_inst
描述：龙虎榜机构成交明细
限量：单次最大10000
积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
trade_date	str	Y	交易日期
ts_code	str	N	TS代码
输出参数

名称	类型	默认显示	描述
trade_date	str	Y	交易日期
ts_code	str	Y	TS代码
exalter	str	Y	营业部名称
side	str	Y	买卖类型0：买入金额最大的前5名， 1：卖出金额最大的前5名
buy	float	Y	买入额（元）
buy_rate	float	Y	买入占总成交比例
sell	float	Y	卖出额（元）
sell_rate	float	Y	卖出占总成交比例
net_buy	float	Y	净成交额（元）
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
    idate = currentDate
    # idate = '20210723'
    itimes = 1;
    while idate <= currentDate:
        if itimes <= 60:
            df = pro.top_inst(trade_date=idate)

            res = df.to_sql('hq_TopInst', engine, index=False, if_exists='append', chunksize=10000,
                            dtype={'trade_date': DATE,
                                   'ts_code': NVARCHAR(20),
                                   'exalter': NVARCHAR(255),
                                   'side': DECIMAL(1),
                                   'buy': DECIMAL(17, 2),
                                   'buy_rate': DECIMAL(17, 2),
                                   'sell': DECIMAL(17, 2),
                                   'sell_rate': DECIMAL(17, 2),
                                   'net_buy': DECIMAL(17, 2),
                                   'reason': NVARCHAR(255),})

            print(itimes, '   sql retrun:', res, '  date:', idate, '  df length:', len(df))
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
        else:
            itimes = 0
            isleeptime = 61
            print('sleep ', isleeptime, 's')
            time.sleep(isleeptime)
        itimes = itimes + 1
    return df

def droptable():
    engine.execute('drop table hq_TopInst')


if __name__ == '__main__':
    # df = read_data()
    engine = init()
    # droptable()
    df = get_and_write_data(engine)
    print(df)
    end_str = input("龙虎榜营业部信息加载完毕，请复核是否正确执行！")
