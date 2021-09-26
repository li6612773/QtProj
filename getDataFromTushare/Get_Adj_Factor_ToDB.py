'''
Created on 2021年7月18日

@author: SJLI

复权因子
接口：adj_factor，可以通过数据工具调试和查看数据。
更新时间：早上9点30分
描述：获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。

输入参数

名称	类型	必选	描述
ts_code	str	Y	股票代码
trade_date	str	N	交易日期(YYYYMMDD，下同)
start_date	str	N	开始日期
end_date	str	N	结束日期
注：日期都填YYYYMMDD格式，比如20181010

输出参数

名称	类型	描述
ts_code	str	股票代码
trade_date	str	交易日期
adj_factor	float	复权因子
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
        if i % 1 == 0 or i+1 == codeList.__len__():
            df = pro.adj_factor(ts_code=codes, start_date=currentDate)

            tosqlret = df.to_sql('hq_adj_factor', engine, chunksize=1000000, if_exists='append', index=False,
                                 dtype={'ts_code': NVARCHAR(20),
                                        'trade_date': DATE,
                                        'adj_factor': DECIMAL(17, 4)})
            # print(tosqlret)
            print(math.ceil(i/1), '/', math.ceil(codeList.__len__()/1), codes, ' ', len(df))

            codes = ''
            itimes = itimes + 1
            if itimes == 200 :
                # time.sleep(61)
                itimes = 0
        i = i + 1
    return df

# def droptable():
    # engine.execute('drop table hq_adj_factor')


if __name__ == '__main__':
    # df = read_data()
    engine = initEngineAndToken()  # 初始化sql引擎和Token数据
    codeList = initCodeList()  # 初始化证券列表
    print('证券列表数量：',codeList.__len__())
    df = get_data_and_toDB(codeList, engine)  # 读取行情数据，并存储到数据库
    print(codeList)
    # print(df)
    end_str = input("当日复权因子加载完成，请复核是否正确执行！")
