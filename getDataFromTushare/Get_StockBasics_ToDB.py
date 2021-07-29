'''
Created on 2021年7月17日

@author: SJLI
'''

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


def read_data():
    sql = """SELECT * FROM stock_basic LIMIT 20"""
    df = pd.read_sql_query(sql, engine)
    return df


def write_data(df,engine):
    res = df.to_sql('hq_stock_basic', engine, index=False, if_exists='append', chunksize=10000,
                    dtype={'ts_code': NVARCHAR(20),
                           'symbol': NVARCHAR(20),
                           'name': NVARCHAR(255),
                           'area': NVARCHAR(50),
                           'industry': NVARCHAR(50),
                           'market': NVARCHAR(50),
                           'list_date': DATE})
    print(res)


def get_data():
    pro = ts.pro_api()
    df = pro.stock_basic()
    return df


def droptable():
    engine.execute('drop table hq_stock_basic')

if __name__ == '__main__':
    # df = read_data()
    engine = init()
    droptable()
    df = get_data()
    write_data(df, engine)
    print(df)
