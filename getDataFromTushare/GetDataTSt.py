import pandas as pd
import tushare as ts
import time
from sqlalchemy import create_engine, String, Float, Column


class Daily():
    """日线行情
    ts_code	str	N	股票代码（二选一）
    trade_date	str	N	交易日期（二选一）
    start_date	str	N	开始日期(YYYYMMDD)
    end_date	str	N	结束日期(YYYYMMDD)
    """
    __tablename__ = 'daily'

    ts_code = Column(String(10), primary_key=True)  # 股票代码
    trade_date = Column(String(8), primary_key=True)  # 交易日期
    open = Column(Float)  # 开盘价
    high = Column(Float)  # 最高价
    low = Column(Float)  # 最低价
    close = Column(Float)  # 收盘价
    pre_close = Column(Float)  # 昨收价
    change = Column(Float)  # 涨跌额
    pct_chg = Column(Float)  # 涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
    vol = Column(Float)  # 成交量 （手）
    amount = Column(Float)  # 成交额 （千元）

class td():
    def get_daily_code(pro, ts_code, start_date, end_date, retry_count=3, pause=2):
        """股票代码方式获取 日线行情 数据"""
        for _ in range(retry_count):
            try:
                df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date,
                               fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')
            except:
                time.sleep(pause)
            else:
                return df


    def get_daily_date(pro, date, retry_count=3, pause=2):
        """日期方式获取 日线行情 数据"""
        for _ in range(retry_count):
            try:
                df = pro.daily(trade_date=date,
                               fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')
            except:
                time.sleep(pause)
            else:
                return df

class mf():
    def get_ts_code(engine):
        """查询ts_code"""
        return pd.read_sql('select ts_code from stock_basic', engine)


    def delete_daily(engine, start_date, end_date):
        """删除 日线行情 数据"""
        conn = engine.connect()
        conn.execute('delete from daily where  trade_date between ' + start_date + ' and ' + end_date)


    def update_all_daily(engine, pro, codes, start_date, end_date, retry_count, pause):
        """股票代码方式更新 日线行情"""
        for value in codes['ts_code']:
            df = td.get_daily_code(pro, value, start_date, end_date, retry_count, pause)
            df.to_sql('daily', engine, if_exists='append', index=False)
            time.sleep(0.6)


    def update_daily_date(engine, pro, date, retry_count, pause):
        """日期方式更新 日线行情"""
        df = td.get_daily_date(pro, date, retry_count, pause)
        df.to_sql('daily', engine, if_exists='append', index=False)


if __name__ == "__main__":
    # 初始化pro接口
    token = '416ae9fd9b5e3c1180827bd24f729982057f67cbfd75af9df7fe9a87'
    print(ts.__version__)
    # 设置token
    ts.set_token(token)
    pro = ts.pro_api()

    # 初始化数据库
    connect_info = 'mysql+pymysql://root:12345@localhost:3306/qtdb?charset=utf8'
    engine = create_engine(connect_info)  # use sqlalchemy to build link-engine

    print(engine, '数据库链接初始化成功')

    # 如果上一步骤ts.set_token('your token')无效或不想保存token到本地，也可以在初始化接口里直接设置token:
    # 根据需要增删 日线行情 数据  单次提取*4000*条
    mf.delete_daily(engine, '19901219', '20191231')
    codes = mf.get_ts_code(engine)
    mf.update_all_daily(engine, pro, codes, '19901219', '20051231', 3, 2)
    mf.update_daily_date(engine, pro, '20190702', 3, 2)
