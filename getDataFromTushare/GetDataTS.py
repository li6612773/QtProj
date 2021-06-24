import tushare as ts
import pandas as pd
import pymysql



from sqlalchemy import create_engine

# 初始化pro接口
token = '416ae9fd9b5e3c1180827bd24f729982057f67cbfd75af9df7fe9a87'
print(ts.__version__)
# 设置token
ts.set_token(token)
pro = ts.pro_api()

codeList = '300541.SZ,300624.SZ'
df = pro.daily(ts_code=codeList, start_date='20180701', end_date='20210611')

# 初始化数据库
connect_info = 'mysql+pymysql://root:12345@localhost:3306/qtdb' \
               ''
engine = create_engine(connect_info)  # use sqlalchemy to build link-engine

print(engine, '数据库链接初始化成功')

print("**************************************************************")
print(df)
# mydf = pd.DataFrame(df, index=df.Fields, columns=df.Codes)
mydf = df
print("to str_________________________________")
mytable = mydf
mytable.insert(0, 'index', mytable.index)
'''
for k in range(0, len(mytable.Fields)):
    print(indata.Fields[k] + "  ")
    for j in range(0, len(mytable.)):
        print(str(indata.Data[k][j]) + ",")
    print("\n")
    '''
print("to db_________________________________")
# write df to table
tosqlret = mytable.to_sql('hq_daily', engine, chunksize=1000000, if_exists='append', index=False)
print(tosqlret)


