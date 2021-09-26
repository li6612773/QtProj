'''
Created on 2021年7月18日

@author: SJLI

每日指标
接口：daily_basic，可以通过数据工具调试和查看数据。
更新时间：交易日每日15点～17点之间
描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
积分：用户需要至少600积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
ts_code	str	Y	股票代码（二选一）
trade_date	str	N	交易日期 （二选一）
start_date	str	N	开始日期(YYYYMMDD)
end_date	str	N	结束日期(YYYYMMDD)
注：日期都填YYYYMMDD格式，比如20181010

输出参数

名称	类型	描述
ts_code	str	TS股票代码
trade_date	str	交易日期
close	float	当日收盘价
turnover_rate	float	换手率（%）
turnover_rate_f	float	换手率（自由流通股）
volume_ratio	float	量比
pe	float	市盈率（总市值/净利润， 亏损的PE为空）
pe_ttm	float	市盈率（TTM，亏损的PE为空）
pb	float	市净率（总市值/净资产）
ps	float	市销率
ps_ttm	float	市销率（TTM）
dv_ratio	float	股息率 （%）
dv_ttm	float	股息率（TTM）（%）
total_share	float	总股本 （万股）
float_share	float	流通股本 （万股）
free_share	float	自由流通股本 （万）
total_mv	float	总市值 （万元）
circ_mv	float	流通市值（万元）
接口用法


pro = ts.pro_api()

df = pro.daily_basic(ts_code='', trade_date='20180726', fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb')

抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。
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
            df = pro.daily_basic(ts_code=codes, start_date=currentDate, end_date=currentDate)

            tosqlret = df.to_sql('hq_daily_basic', engine, chunksize=1000000, if_exists='append', index=False,
                                 dtype={'ts_code': NVARCHAR(20),
                                        'trade_date': DATE,
                                        'close': DECIMAL(17, 2),
                                        'turnover_rate': DECIMAL(17, 2),
                                        'turnover_rate_f': DECIMAL(17, 2),
                                        'volume_ratio': DECIMAL(17, 2),
                                        'pe': DECIMAL(17, 2),
                                        'pe_ttm': DECIMAL(17, 2),
                                        'pb': DECIMAL(17, 2),
                                        'ps': DECIMAL(17, 2),
                                        'ps_ttm': DECIMAL(17, 2),
                                        'dv_ratio': DECIMAL(17, 2),
                                        'dv_ttm': DECIMAL(17, 2),
                                        'total_share': DECIMAL(17, 2),
                                        'float_share': DECIMAL(17, 2),
                                        'free_share': DECIMAL(17, 2),
                                        'total_mv': DECIMAL(17, 2),
                                        'circ_mv': DECIMAL(17, 2),

})
            # print(tosqlret)
            print(math.ceil(i/12), '/', math.ceil(codeList.__len__()/1), codes, ' ', len(df))

            codes = ''
            itimes = itimes + 1
            if itimes == 200 :
                time.sleep(61)
                itimes = 0
        i = i + 1
    return df

# def droptable():
    # engine.execute('drop table hq_daily_basic')


if __name__ == '__main__':
    # df = read_data()
    engine = initEngineAndToken()  # 初始化sql引擎和Token数据
    codeList = initCodeList()  # 初始化证券列表
    print('证券列表数量：',codeList.__len__())
    df = get_data_and_toDB(codeList, engine)  # 读取行情数据，并存储到数据库
    print(codeList)
    # print(df)
    end_str = input("当日每日指标加载完成，请复核是否正确执行！")
