'''
Created on 2021年8月04日

@author: SJLI

交易日历
接口：trade_cal
描述：获取各大交易所交易日历数据,默认提取的是上交所

输入参数

名称	类型	必选	描述
exchange	str	N	交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源,IB 银行间,XHKG 港交所
start_date	str	N	开始日期 （格式：YYYYMMDD 下同）
end_date	str	N	结束日期
is_open	str	N	是否交易 '0'休市 '1'交易
输出参数

名称	类型	默认显示	描述
exchange	str	Y	交易所 SSE上交所 SZSE深交所
cal_date	str	Y	日历日期
is_open	str	Y	是否交易 0休市 1交易
pretrade_date	str	N	上一个交易日
接口示例


pro = ts.pro_api()


df = pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')
或者


df = pro.query('trade_cal', start_date='20180101', end_date='20181231')
'''
import datetime

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
    res = df.to_sql('hq_trade_cal', engine, index=False, if_exists='append', chunksize=10000,
                    dtype={'exchange': NVARCHAR(20),
                           'cal_date': DATE,
                           'is_open': NVARCHAR(1)})
    print(res)


def get_data():
    currentDate = datetime.datetime.now().strftime('%Y%m%d')
    pro = ts.pro_api()
    df = pro.query('trade_cal', start_date='20050101', end_date='20211231')
    return df


def droptable():
    engine.execute('drop table if exists  hq_trade_cal')

if __name__ == '__main__':
    # df = read_data()
    engine = init()
    droptable()
    df = get_data()
    write_data(df, engine)
    print(df)
    end_str = input("交易日历更新完毕，请复核是否正确执行！")
