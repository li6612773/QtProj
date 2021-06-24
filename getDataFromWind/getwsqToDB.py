# coding: UTF-8
# 说明：
# 该案例是演示wsq实时行情订阅的使用，订阅模式主要有两部分组成，一部分是用wsq函数订阅所需要的行情，
# 另一部分是编写自己的回调函数，用于处理实时推送过来的行情数据
# myCallback(indata) 即为本案例所使用的回调函数，回调函数有且只能有一个参数：indata
# indata的数据结构如下：
# indata.ErrorCode 错误码，如果为0表示运行正常
# indata.StateCode 状态字段，使用时无需处理
# indata.RequestID 存放对应wsq请求的RequestID
# indata.Codes 存放行情对应的code
# indata.Fields 存放行情数据对应的指标
# indata.Times 存放本地时间，注意这个不是行情对应的时间，要获取行情对应的时间，请订阅rt_time指标
# indata.Data 存放行情数据

# 取消订阅可使用w.cancelRequest(requestID),如果想取消全部订阅，可使用w.cancelRequest(0)

# 例如:
# indata.ErrorCode=0
# indata.StateCode=1
# indata.RequestID=3
# indata.Codes=[IF.CFE]
# indata.Fields=[RT_LAST]
# indata.Times=[20151123 15:12:40]
# indata.Data=[[3623.0]]
from urllib.request import localhost

from WindPy import *
import pymysql.cursors
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

connect_info = 'mysql+pymysql://qtadmin:admin558@localhost:3306/qt_schema?charset=utf8'
engine = create_engine(connect_info)  # use sqlalchemy to build link-engine

'''
# 连接数据库
connect = pymysql.Connect(
    host='localhost',
    port=3306,
    user='qtadmin',
    passwd='admin558',
    db='qt_schema',
    charset='utf8'
)
'''
# 获取游标
# cursor = connect.cursor()

# initdb 创建数据表qt_wsq用于存储实时行情
'''
cursor.execute("""
CREATE TABLE qt_wsq (
    stock_code  varchar(255) NULL ,
    rt_date  date NULL ,
    rt_time  time NULL ,
    rt_ask1  float NULL ,
    rt_ask2 float NULL ,
    rt_ask3 float NULL ,
    rt_bid1 float NULL ,
    rt_bid2 float NULL ,
    rt_bid3 float NULL ,
    rt_bsize1 decimal NULL,
    rt_bsize2 decimal NULL,
    rt_bsize3 decimal NULL,
    rt_asize1 decimal NULL, 
    rt_asize2 decimal NULL,
    rt_asize3 decimal NULL)
""");
'''

mydf = ''


# open a file to write.
# pf = open('d:\\pywsqdataif.data', 'w')

# define the callback function
# 用于处理行情的回调函数
def myCallback(indata):
    print("**************************************************************")
    print(indata)
    if indata.ErrorCode != 0:
        print('error code:' + str(indata.ErrorCode) + '\n');
        return ();
    print("dataframe_________________________________")
    # print("function my_callback was called with %s input" % (indata))
    mydf = pd.DataFrame(indata.Data, index=indata.Fields, columns=indata.Codes)

    print("to str_________________________________")
    mytable = mydf.T
    mytable.insert(0, 'code', mytable.index)
    '''
    for k in range(0, len(mytable.Fields)):
        print(indata.Fields[k] + "  ")
        for j in range(0, len(mytable.)):
            print(str(indata.Data[k][j]) + ",")
        print("\n")
        '''
    print("to db_________________________________")
    # write df to table
    tosqlret = mytable.to_sql('qt_wsqauto', engine, chunksize=1000, if_exists='append', index=False)
    print(tosqlret)

    '''
    all_cells.to_sql(name='cells_fee', con=engine, chunksize=1000, if_exists='replace', index=None)
    对于DataFrame的to_sql函数，需要注意的参数在代码中已经写出来，其中比较重要的是chunksize、if_exists和index。
    chunksize可以设置一次入库的大小；if_exists设置如果数据库中存在同名表怎么办，
    ‘replace’表示将表原来数据删除放入当前数据；
    ‘append’表示追加；‘fail’则表示将抛出异常，结束操作，默认是‘fail’；index=接受boolean值，
    表示是否将DataFrame的index也作为表的列存储。
    '''
    # print("str_________________________________")
    '''
    global begintime
    lastvalue = ""
    for k in range(0, len(indata.Fields)):
        if indata.Fields[k] == "RT_TIME":
            begintime = indata.Data[k][0]
        if indata.Fields[k] == "RT_LAST":
            lastvalue = str(indata.Data[k][0])
        if indata.Fields[k] == "RT_BSIZE1":
                lastvalue = str(indata.Data[k][0])
              
    string = str(begintime) + " " + lastvalue + "\n"
    #    pf.writelines(string)
    print(string)        
    '''


#    pf.flush();
# 插入数据
'''
sql = "INSERT INTO trade (name, account, saving) VALUES ( '%s', '%s', %.2f )"
data = ('雷军', '13512345678', 10000)
cursor.execute(sql % data)
connect.commit()
print('成功插入', cursor.rowcount, '条数据')
'''
# 想要结束订阅，可使用w.cancelRequest(0)命令，然后后调用pf.close()关闭文件
# pf.close();


w.start();
# 订阅行情
'''
wsqdata1 = w.wsq("000001.SZ,000002.SZ,000004.SZ",
                 "rt_date,rt_time,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,"
                 "rt_asize1, rt_asize2,rt_asize3",
                 func=myCallback)

w.wsq("000001.SZ,000002.SZ,000063.SZ,000069.SZ,000100.SZ,000157.SZ,000166.SZ,000333.SZ,000338.SZ,000402.SZ,000408.SZ,"
      "000413.SZ,000415.SZ,000423.SZ,000425.SZ,000538.SZ,000553.SZ,000568.SZ,000596.SZ,000625.SZ,000627.SZ,000629.SZ,"
      "000630.SZ,000651.SZ,000656.SZ,000661.SZ,000671.SZ,000703.SZ,000709.SZ,000725.SZ,000728.SZ,000768.SZ,000776.SZ,"
      "000783.SZ,000786.SZ,000858.SZ,000876.SZ,000895.SZ,000898.SZ,000938.SZ,000961.SZ,000963.SZ,001979.SZ,002001.SZ,"
      "002007.SZ,002008.SZ,002010.SZ,002024.SZ,002027.SZ,002032.SZ,002044.SZ,002050.SZ,002065.SZ,002081.SZ,002120.SZ,"
      
      "002142.SZ,002146.SZ,002153.SZ,002179.SZ,002202.SZ,002230.SZ,002236.SZ,002241.SZ,002252.SZ,002271.SZ,002294.SZ,"
      
      "002304.SZ,002310.SZ,002311.SZ,002352.SZ,002410.SZ,002411.SZ,002415.SZ,002422.SZ,002456.SZ,002460.SZ,002466.SZ,"
      "002468.SZ,002475.SZ,002493.SZ,002508.SZ,002555.SZ,002558.SZ,002594.SZ,002601.SZ,002602.SZ,002624.SZ,002625.SZ,"
      "002673.SZ,002714.SZ,002736.SZ,002739.SZ,002773.SZ,002925.SZ,002938.SZ,002939.SZ,002945.SZ,300003.SZ,300015.SZ,"
      "300017.SZ,300024.SZ,300033.SZ,300059.SZ,300070.SZ,300072.SZ,300122.SZ,300124.SZ,300136.SZ,300142.SZ,300144.SZ,"
      "300251.SZ,300296.SZ,300408.SZ,300413.SZ,300433.SZ,300498.SZ,600000.SH,600004.SH,600009.SH,600010.SH,600011.SH,"
      "600015.SH,600016.SH,600018.SH,600019.SH,600023.SH,600025.SH,600027.SH,600028.SH,600029.SH,600030.SH,600031.SH,"
      "600036.SH,600038.SH,600048.SH,600050.SH,600061.SH,600066.SH,600068.SH,600085.SH,600089.SH,600100.SH,600104.SH,"
      "600109.SH,600111.SH,600115.SH,600118.SH,600153.SH,600170.SH,600176.SH,600177.SH,600188.SH,600196.SH,600208.SH,"
      "600219.SH,600221.SH,600233.SH,600271.SH,600276.SH,600297.SH,600299.SH,600309.SH,600332.SH,600339.SH,600340.SH,"
      "600346.SH,600352.SH,600362.SH,600369.SH,600372.SH,600383.SH,600390.SH,600398.SH,600406.SH,600415.SH,600436.SH,"
      "600438.SH,600482.SH,600487.SH,600489.SH,600498.SH,600516.SH,600519.SH,600522.SH,600535.SH,600547.SH,600566.SH,"
      "600570.SH,600583.SH,600585.SH,600588.SH,600606.SH,600637.SH,600660.SH,600663.SH,600674.SH,600688.SH,600690.SH,"
      "600703.SH,600704.SH,600705.SH,600733.SH,600741.SH,600760.SH,600795.SH,600809.SH,600816.SH,600837.SH,600867.SH,"
      "600886.SH,600887.SH,600893.SH,600900.SH,600919.SH,600926.SH,600958.SH,600977.SH,600998.SH,600999.SH,601006.SH,"
      "601009.SH,601012.SH,601018.SH,601021.SH,601066.SH,601088.SH,601108.SH,601111.SH,601117.SH,601138.SH,601155.SH,"
      "601162.SH,601166.SH,601169.SH,601186.SH,601198.SH,601211.SH,601212.SH,601216.SH,601225.SH,601228.SH,601229.SH,"
      "601238.SH,601288.SH,601298.SH,601318.SH,601319.SH,601328.SH,601336.SH,601360.SH,601377.SH,601390.SH,601398.SH,"
      "601555.SH,601577.SH,601600.SH,601601.SH,601607.SH,601618.SH,601628.SH,601633.SH,601668.SH,601669.SH,601688.SH,"
      "601727.SH,601766.SH,601788.SH,601800.SH,601808.SH,601818.SH,601828.SH,601838.SH,601857.SH,601877.SH,601878.SH,"
      "601881.SH,601888.SH,601898.SH,601899.SH,601901.SH,601919.SH,601933.SH,601939.SH,601985.SH,601988.SH,601989.SH,"
      "601992.SH,601997.SH,601998.SH,603019.SH,603156.SH,603160.SH,603259.SH,603260.SH,603288.SH,603799.SH,603833.SH,"
      "603858.SH,603986.SH,603993.SH", "rt_date,rt_time,rt_pre_close,rt_open,rt_high,rt_low,rt_last,rt_last_amt,"
                                       "rt_last_vol,rt_latest,rt_vol,rt_amt,rt_chg,rt_pct_chg,rt_high_limit,"
                                       "rt_low_limit,rt_swing,rt_vwap,rt_upward_vol,rt_downward_vol,rt_trade_status,"
                                       "rt_high_52wk,rt_low_52wk,rt_pct_chg_1min,rt_pct_chg_3min,rt_pct_chg_5min,"
                                       "rt_pct_chg_5d,rt_pct_chg_10d,rt_pct_chg_20d,rt_pct_chg_60d,rt_pct_chg_120d,"
                                       "rt_pct_chg_250d,rt_pct_chg_ytd,rt_ask1,rt_ask2,rt_ask3,rt_ask4,rt_ask5,"
                                       "rt_ask6,rt_ask7,rt_ask8,rt_ask9,rt_ask10,rt_bid1,rt_bid2,rt_bid3,rt_bid4,"
                                       "rt_bid5,rt_bid6,rt_bid7,rt_bid8,rt_bid9,rt_bid10,rt_bsize1,rt_bsize2,"
                                       "rt_bsize3,rt_bsize4,rt_bsize5,rt_bsize6,rt_bsize7,rt_bsize8,rt_bsize9,"
                                       "rt_bsize10,rt_asize1,rt_asize2,rt_asize3,rt_asize4,rt_asize5,rt_asize6,"
                                       "rt_asize7,rt_asize8,rt_asize9,rt_asize10", func=myCallback)
'''
w.wsq("000001.SZ,000002.SZ,000063.SZ,000069.SZ,000100.SZ,000157.SZ,000166.SZ,000333.SZ,000338.SZ,000402.SZ,000408.SZ,"
      "000413.SZ,000415.SZ,000423.SZ,000425.SZ,000538.SZ,000553.SZ,000568.SZ,000596.SZ,000625.SZ,000627.SZ,000629.SZ,"
      "000630.SZ,000651.SZ,000656.SZ,000661.SZ,000671.SZ,000703.SZ,000709.SZ,000725.SZ,000728.SZ,000768.SZ,000776.SZ,"
      "000783.SZ,000786.SZ,000858.SZ,000876.SZ,000895.SZ,000898.SZ,000938.SZ,000961.SZ,000963.SZ,001979.SZ,002001.SZ,"
      "002007.SZ,002008.SZ,002010.SZ,002024.SZ,002027.SZ,002032.SZ,002044.SZ,002050.SZ,002065.SZ,002081.SZ,002120.SZ,",
      "rt_date,rt_time,rt_open,rt_high,rt_low,rt_last,rt_last_amt,rt_last_vol,rt_latest,"
      "rt_vol,rt_amt,rt_chg,rt_pct_chg,rt_high_limit,rt_low_limit,rt_swing,rt_vwap,"
      "rt_upward_vol,rt_downward_vol,rt_pct_chg_1min,rt_pct_chg_3min,rt_pct_chg_5min,"
      "rt_pct_chg_5d,rt_pct_chg_10d,rt_ask1,rt_ask2,rt_ask3,rt_ask4,rt_ask5,rt_ask6,rt_ask7,"
      "rt_ask8,rt_ask9,rt_ask10,rt_bid1,rt_bid2,rt_bid3,rt_bid4,rt_bid5,rt_bid6,rt_bid7,"
      "rt_bid8,rt_bid9,rt_bid10,rt_bsize1,rt_bsize2,rt_bsize3,rt_bsize4,rt_bsize5,rt_bsize6,"
      "rt_bsize7,rt_bsize8,rt_bsize9,rt_bsize10,rt_asize1,rt_asize2,rt_asize3,rt_asize4,"
      "rt_asize5,rt_asize6,rt_asize7,rt_asize8,rt_asize9,rt_asize10", func=myCallback)
# w.wsq("IF.CFE","rt_time,rt_last",func=myCallback)
# while (1):
#    info = "这个while循环主要是防止IDE在运行或者debug时，运行w.wsq()语句后就退出，从而导致行情推送过来后，回调函数无法运行！";
# for i in range(0,100):
while (1):
    info = "这个while循环主要是防止IDE在运行或者debug时，运行w.wsq()语句后就退出，从而导致行情推送过来后，回调函数无法运行！";
