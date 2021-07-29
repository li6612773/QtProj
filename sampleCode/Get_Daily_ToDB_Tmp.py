import tushare as ts
import pandas as pd
import pymysql

from sqlalchemy import create_engine
#初始化证券列表
codeList = ['002536.SZ']
codeList.append('000004.SZ')
codeList.append('002895.SZ')
codeList.append('300345.SZ')
codeList.append('300339.SZ')
codeList.append('300220.SZ')
codeList.append('000683.SZ')
codeList.append('603332.SZ')
codeList.append('300541.SZ')
codeList.append('300624.SZ')
codeList.append('300608.SZ')
codeList.append('600355.SZ')
codeList.append('600732.SZ')
codeList.append('688118.SZ')
codeList.append('600032.SZ')
codeList.append('002617.SZ')
codeList.append('300079.SZ')
codeList.append('300339.SZ')
codeList.append('300345.SZ')
codeList.append('600732.SZ')
codeList.append('000572.SZ')
codeList.append('002759.SZ')
codeList.append('300998.SZ')
codeList.append('688565.SZ')
codeList.append('300339.SZ')
codeList.append('300663.SZ')
codeList.append('300234.SZ')
codeList.append('000982.SZ')
codeList.append('002137.SZ')
codeList.append('600982.SZ')
codeList.append('601339.SZ')
codeList.append('002881.SZ')
codeList.append('002326.SZ')
codeList.append('603927.SZ')
codeList.append('601127.SZ')
codeList.append('002892.SZ')
codeList.append('001896.SZ')
codeList.append('600906.SZ')
codeList.append('603650.SZ')
codeList.append('605117.SZ')
codeList.append('300264.SZ')
codeList.append('300393.SZ')
codeList.append('688022.SZ')
codeList.append('300077.SZ')
codeList.append('300581.SZ')
codeList.append('600070.SZ')
codeList.append('002273.SZ')
codeList.append('002284.SZ')
codeList.append('002010.SZ')
codeList.append('600905.SZ')
codeList.append('600753.SZ')
codeList.append('603389.SZ')
codeList.append('300767.SZ')
codeList.append('002075.SZ')
codeList.append('688607.SZ')
codeList.append('002249.SZ')
codeList.append('300108.SZ')
codeList.append('600839.SZ')
codeList.append('300153.SZ')
codeList.append('600211.SZ')
codeList.append('300108.SZ')
codeList.append('000625.SZ')
codeList.append('605300.SZ')
codeList.append('000150.SZ')
codeList.append('000158.SZ')
codeList.append('300554.SZ')
codeList.append('300872.SZ')
codeList.append('600775.SZ')
codeList.append('002709.SZ')
codeList.append('003027.SZ')
codeList.append('605089.SZ')
codeList.append('002626.SZ')
codeList.append('002908.SZ')
codeList.append('002622.SZ')
codeList.append('000592.SZ')
codeList.append('002956.SZ')
codeList.append('688559.SZ')
codeList.append('688221.SZ')
codeList.append('688336.SZ')
codeList.append('300945.SZ')
codeList.append('300309.SZ')
codeList.append('300943.SZ')
codeList.append('600439.SZ')
codeList.append('600331.SZ')
codeList.append('600965.SZ')
codeList.append('000650.SZ')
codeList.append('000829.SZ')
codeList.append('600095.SZ')
codeList.append('002922.SZ')
codeList.append('002173.SZ')
codeList.append('300477.SZ')
codeList.append('300204.SZ')
codeList.append('300264.SZ')
codeList.append('300955.SZ')
codeList.append('600744.SZ')
codeList.append('603178.SZ')
codeList.append('000966.SZ')
codeList.append('000066.SZ')
codeList.append('000718.SZ')
codeList.append('605016.SZ')
codeList.append('605108.SZ')
codeList.append('600753.SZ')
codeList.append('000012.SZ')
codeList.append('603657.SZ')
codeList.append('603518.SZ')
codeList.append('605117.SZ')
codeList.append('003039.SZ')
codeList.append('002471.SZ')
codeList.append('002613.SZ')
codeList.append('600864.SZ')
codeList.append('603933.SZ')
codeList.append('600137.SZ')
codeList.append('600327.SZ')
codeList.append('002235.SZ')
codeList.append('603025.SZ')
codeList.append('600389.SZ')
codeList.append('002728.SZ')
codeList.append('603726.SZ')
codeList.append('605299.SZ')
codeList.append('603787.SZ')
codeList.append('002922.SZ')
codeList.append('300264.SZ')
codeList.append('000408.SZ')
codeList.append('000966.SZ')
codeList.append('000928.SZ')
codeList.append('300056.SZ')
codeList.append('300631.SZ')
codeList.append('003027.SZ')
codeList.append('600733.SZ')
codeList.append('000625.SZ')
codeList.append('000800.SZ')
codeList.append('600006.SZ')
codeList.append('002537.SZ')
codeList.append('000928.SZ')
codeList.append('002615.SZ')
codeList.append('002490.SZ')
codeList.append('600137.SZ')
codeList.append('600439.SZ')
codeList.append('003016.SZ')
codeList.append('002812.SZ')
codeList.append('300268.SZ')
codeList.append('000635.SZ')
codeList.append('002407.SZ')
codeList.append('000158.SZ')
codeList.append('003020.SZ')
codeList.append('002536.SZ')
codeList.append('002581.SZ')
codeList.append('000532.SZ')
codeList.append('600321.SZ')
codeList.append('300715.SZ')
codeList.append('688510.SZ')
codeList.append('688068.SZ')
codeList.append('300077.SZ')
codeList.append('300884.SZ')
codeList.append('300268.SZ')
codeList.append('300340.SZ')
codeList.append('688097.SZ')
codeList.append('605378.SZ')
codeList.append('003027.SZ')
codeList.append('002679.SZ')
codeList.append('002379.SZ')
codeList.append('600906.SZ')
codeList.append('603787.SZ')
codeList.append('603722.SZ')
codeList.append('600616.SZ')
codeList.append('002759.SZ')
codeList.append('605186.SZ')
codeList.append('603196.SZ')
codeList.append('002679.SZ')
codeList.append('688598.SZ')
codeList.append('300220.SZ')
codeList.append('603803.SZ')
codeList.append('688609.SZ')
codeList.append('000572.SZ')
codeList.append('002902.SZ')
codeList.append('603887.SZ')
codeList.append('002536.SH')
codeList.append('000004.SH')
codeList.append('002895.SH')
codeList.append('300345.SH')
codeList.append('300339.SH')
codeList.append('300220.SH')
codeList.append('000683.SH')
codeList.append('603332.SH')
codeList.append('300541.SH')
codeList.append('300624.SH')
codeList.append('300608.SH')
codeList.append('600355.SH')
codeList.append('600732.SH')
codeList.append('688118.SH')
codeList.append('600032.SH')
codeList.append('002617.SH')
codeList.append('300079.SH')
codeList.append('300339.SH')
codeList.append('300345.SH')
codeList.append('600732.SH')
codeList.append('000572.SH')
codeList.append('002759.SH')
codeList.append('300998.SH')
codeList.append('688565.SH')
codeList.append('300339.SH')
codeList.append('300663.SH')
codeList.append('300234.SH')
codeList.append('000982.SH')
codeList.append('002137.SH')
codeList.append('600982.SH')
codeList.append('601339.SH')
codeList.append('002881.SH')
codeList.append('002326.SH')
codeList.append('603927.SH')
codeList.append('601127.SH')
codeList.append('002892.SH')
codeList.append('001896.SH')
codeList.append('600906.SH')
codeList.append('603650.SH')
codeList.append('605117.SH')
codeList.append('300264.SH')
codeList.append('300393.SH')
codeList.append('688022.SH')
codeList.append('300077.SH')
codeList.append('300581.SH')
codeList.append('600070.SH')
codeList.append('002273.SH')
codeList.append('002284.SH')
codeList.append('002010.SH')
codeList.append('600905.SH')
codeList.append('600753.SH')
codeList.append('603389.SH')
codeList.append('300767.SH')
codeList.append('002075.SH')
codeList.append('688607.SH')
codeList.append('002249.SH')
codeList.append('300108.SH')
codeList.append('600839.SH')
codeList.append('300153.SH')
codeList.append('600211.SH')
codeList.append('300108.SH')
codeList.append('000625.SH')
codeList.append('605300.SH')
codeList.append('000150.SH')
codeList.append('000158.SH')
codeList.append('300554.SH')
codeList.append('300872.SH')
codeList.append('600775.SH')
codeList.append('002709.SH')
codeList.append('003027.SH')
codeList.append('605089.SH')
codeList.append('002626.SH')
codeList.append('002908.SH')
codeList.append('002622.SH')
codeList.append('000592.SH')
codeList.append('002956.SH')
codeList.append('688559.SH')
codeList.append('688221.SH')
codeList.append('688336.SH')
codeList.append('300945.SH')
codeList.append('300309.SH')
codeList.append('300943.SH')
codeList.append('600439.SH')
codeList.append('600331.SH')
codeList.append('600965.SH')
codeList.append('000650.SH')
codeList.append('000829.SH')
codeList.append('600095.SH')
codeList.append('002922.SH')
codeList.append('002173.SH')
codeList.append('300477.SH')
codeList.append('300204.SH')
codeList.append('300264.SH')
codeList.append('300955.SH')
codeList.append('600744.SH')
codeList.append('603178.SH')
codeList.append('000966.SH')
codeList.append('000066.SH')
codeList.append('000718.SH')
codeList.append('605016.SH')
codeList.append('605108.SH')
codeList.append('600753.SH')
codeList.append('000012.SH')
codeList.append('603657.SH')
codeList.append('603518.SH')
codeList.append('605117.SH')
codeList.append('003039.SH')
codeList.append('002471.SH')
codeList.append('002613.SH')
codeList.append('600864.SH')
codeList.append('603933.SH')
codeList.append('600137.SH')
codeList.append('600327.SH')
codeList.append('002235.SH')
codeList.append('603025.SH')
codeList.append('600389.SH')
codeList.append('002728.SH')
codeList.append('603726.SH')
codeList.append('605299.SH')
codeList.append('603787.SH')
codeList.append('002922.SH')
codeList.append('300264.SH')
codeList.append('000408.SH')
codeList.append('000966.SH')
codeList.append('000928.SH')
codeList.append('300056.SH')
codeList.append('300631.SH')
codeList.append('003027.SH')
codeList.append('600733.SH')
codeList.append('000625.SH')
codeList.append('000800.SH')
codeList.append('600006.SH')
codeList.append('002537.SH')
codeList.append('000928.SH')
codeList.append('002615.SH')
codeList.append('002490.SH')
codeList.append('600137.SH')
codeList.append('600439.SH')
codeList.append('003016.SH')
codeList.append('002812.SH')
codeList.append('300268.SH')
codeList.append('000635.SH')
codeList.append('002407.SH')
codeList.append('000158.SH')
codeList.append('003020.SH')
codeList.append('002536.SH')
codeList.append('002581.SH')
codeList.append('000532.SH')
codeList.append('600321.SH')
codeList.append('300715.SH')
codeList.append('688510.SH')
codeList.append('688068.SH')
codeList.append('300077.SH')
codeList.append('300884.SH')
codeList.append('300268.SH')
codeList.append('300340.SH')
codeList.append('688097.SH')
codeList.append('605378.SH')
codeList.append('003027.SH')
codeList.append('002679.SH')
codeList.append('002379.SH')
codeList.append('600906.SH')
codeList.append('603787.SH')
codeList.append('603722.SH')
codeList.append('600616.SH')
codeList.append('002759.SH')
codeList.append('605186.SH')
codeList.append('603196.SH')
codeList.append('002679.SH')
codeList.append('688598.SH')
codeList.append('300220.SH')
codeList.append('603803.SH')
codeList.append('688609.SH')
codeList.append('000572.SH')
codeList.append('002902.SH')
codeList.append('603887.SH')



# 初始化pro接口
token = '416ae9fd9b5e3c1180827bd24f729982057f67cbfd75af9df7fe9a87'
print(ts.__version__)
# 设置token
ts.set_token(token)
pro = ts.pro_api()

# 初始化数据库
connect_info = 'mysql+pymysql://root:12345@localhost:3306/qtdb' \
               ''
engine = create_engine(connect_info)  # use sqlalchemy to build link-engine

print(engine, '数据库链接初始化成功')

for code in codeList:
    df = pro.daily(ts_code=code, start_date='20180701', end_date='20210625')

    print("**************************************************************")
    #print(df)
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







