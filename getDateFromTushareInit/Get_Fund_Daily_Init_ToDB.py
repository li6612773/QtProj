'''
Created on 2021年10月18日

@author: SJLI

场内基金日线行情
接口：fund_daily
描述：获取场内基金日线行情，类似股票日行情
更新：每日收盘后2小时内
限量：单次最大800行记录，总量不限制
积分：用户需要至少500积分才可以调取，具体请参阅积分获取办法

复权行情实现参考：

后复权 = 当日最新价 × 当日复权因子
前复权 = 当日复权价 ÷ 最新复权因子

输入参数

名称	类型	必选	描述
ts_code	str	N	基金代码（二选一）
trade_date	str	N	交易日期（二选一）
start_date	str	N	开始日期
end_date	str	N	结束日期
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS代码
trade_date	str	Y	交易日期
open	float	Y	开盘价(元)
high	float	Y	最高价(元)
low	float	Y	最低价(元)
close	float	Y	收盘价(元)
pre_close	float	Y	昨收盘价(元)
change	float	Y	涨跌额(元)
pct_chg	float	Y	涨跌幅(%)
vol	float	Y	成交量(手)
amount	float	Y	成交额(千元)
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
    sql = """select ts_code,list_date from hq_fund_basic where market = 'E' order by ts_code"""
    codeList = pd.read_sql_query(sql, engine)
    return codeList


def get_data_and_toDB(codeList, engine,startDate,endDate):
    pro = ts.pro_api()
    startDateStr  = startDate.strftime('%Y%m%d')
    endDateStr  = endDate.strftime('%Y%m%d')

    codeListArray = codeList.__array__()
    codes = ''
    i = 0
    itimes = 0
    icode = 0
    ncode = 1 #一次读取多少只证券的信息
    for code, list_date in codeListArray:
        # ipoDate = datetime.date.strftime(list_date, '%Y%m%d')
        if codes == '':
            codes = code
        else:
            codes = codes + ',' + code
        if i % ncode == 0 or i+1 == codeList.__len__():
            df = pro.fund_daily(ts_code=codes, start_date=startDateStr, end_date=endDateStr)

            tosqlret = df.to_sql('hq_fund_daily', engine, chunksize=1000000, if_exists='append', index=False,
                                 dtype={'ts_code': NVARCHAR(20),
                                        'trade_date': DATE,
                                        'pre_close': DECIMAL(17, 2),
                                        'open': DECIMAL(17, 2),
                                        'high': DECIMAL(17, 2),
                                        'low': DECIMAL(17, 2),
                                        'close': DECIMAL(17, 2),
                                        'change': DECIMAL(17, 2),
                                        'pct_chg': DECIMAL(17, 2),
                                        'vol': DECIMAL(17, 2),
                                        'amount': DECIMAL(17, 2)})
            # print(tosqlret)
            print(math.ceil(i/ncode), '/', math.ceil(codeList.__len__()/ncode), codes, ' ', len(df))

            codes = ''
            itimes = itimes + 1
            if itimes == 200 :
                time.sleep(61)
                itimes = 0
        i = i + 1
    return df

# def droptable():
    # engine.execute('drop table hq_fund_daily')


if __name__ == '__main__':
    # df = read_data()
    engine = initEngineAndToken()  # 初始化sql引擎和Token数据
    codeList = initCodeList()  # 初始化证券列表
    print('证券列表数量：',codeList.__len__())
    currentDate = datetime.datetime.now()
    startDate = datetime.datetime.strptime('20200101','%Y%m%d')
    endDate = datetime.datetime.strptime('20201231','%Y%m%d')
    while startDate != endDate:
        print(startDate,'  ',endDate)
        df = get_data_and_toDB(codeList, engine,startDate,endDate)  # 读取行情数据，并存储到数据库
        # print(codeList)
        print(df)
        startDate = startDate + datetime.timedelta(years=1)
        if startDate > currentDate :
            startDate = currentDate
        endDate = endDate + datetime.timedelta(years=1)
        if endDate > currentDate:
            endDate = currentDate
    end_str = input("每日指标初始化完成，请复核是否正确执行！")
