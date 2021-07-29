'''
Created on 2021年7月18日

@author: SJLI

日线行情
接口：daily
数据说明：交易日每天15点～16点之间。本接口是未复权行情，停牌期间不提供数据。
调取说明：基础积分每分钟内最多调取500次，每次5000条数据，相当于23年历史，用户获得超过5000积分正常调取无频次限制。
描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据。

输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码（支持多个股票同时提取，逗号分隔）
trade_date	str	N	交易日期（YYYYMMDD）
start_date	str	N	开始日期(YYYYMMDD)
end_date	str	N	结束日期(YYYYMMDD)
注：日期都填YYYYMMDD格式，比如20181010

输出参数

名称	类型	描述
ts_code	str	股票代码
trade_date	str	交易日期
open	float	开盘价
high	float	最高价
low	float	最低价
close	float	收盘价
pre_close	float	昨收价
change	float	涨跌额
pct_chg	float	涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
vol	float	成交量 （手）
amount	float	成交额 （千元）

'''
import datetime

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
    i = 1
    for code,list_date in codeListArray:
        ipoDate = datetime.date.strftime(list_date,'%Y%m%d')
        df = pro.daily(ts_code=code, start_date=currentDate, end_date=currentDate)
        tosqlret = df.to_sql('hq_daily', engine, chunksize=1000000, if_exists='append', index=False,
                             dtype={'ts_code': NVARCHAR(20),
                                    'trade_date': DATE,
                                    'open': DECIMAL(17, 2),
                                    'high': DECIMAL(17, 2),
                                    'low': DECIMAL(17, 2),
                                    'close': DECIMAL(17, 2),
                                    'pre_close': DECIMAL(17, 2),
                                    'change': DECIMAL(17, 2),
                                    'pct_chg': DECIMAL(17, 2),
                                    'vol': DECIMAL(17, 2),
                                    'amount': DECIMAL(17, 2), })
        #print(tosqlret)
        print(i,'/',codeList.__len__(),code,' ',len(df))
        i=i+1
    return df
    # res = df.to_sql('stock_basic', engine, index=False, if_exists='append', chunksize=5000,
    #                 dtype={'ts_code': NVARCHAR(255),
    #                        'symbol': NVARCHAR(255),
    #                        'name': NVARCHAR(255),
    #                        'area': NVARCHAR(255),
    #                        'industry': NVARCHAR(255),
    #                        'market': NVARCHAR(255),
    #                        'list_date': DATE})

# def droptable():
    # engine.execute('drop table stock_basic')


if __name__ == '__main__':
    # df = read_data()
    engine = initEngineAndToken()  # 初始化sql引擎和Token数据
    codeList = initCodeList()  # 初始化证券列表
    print('证券列表数量：',codeList.__len__())
    df = get_data_and_toDB(codeList, engine)  # 读取行情数据，并存储到数据库
    print(codeList)
    # print(df)
