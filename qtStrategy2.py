import pymysql.cursors

'''
满足以下任一条件即为“满足拉涨”
1）T+1买1价上涨
2）T+1买1价不变  and    T+1卖1量<T卖1量 （说明卖单被吃）
      说明卖单被吃掉 有上涨动力
3）T+1买1价不变  and    T+1买1量>T买1量 （说明狂挂买单了）
4）T+1买1价不变  and    T+1卖1价上涨，说明出现空挡
满足以下任一条件即为“满足放量”
5）T+1成交量》T成交量的n倍

拉涨和交易放量共同满足即可判定为满足买入条件

可以抽象为规则集表：
规则代号，规则参数（；隔开），是否执行

规则公式表：
规则公式，执行结果
'''


class rowForATime:
    def __init__(self, indbRow):
        # 对格子
        self.CODE = indbRow[0]
        self.DATE = indbRow[1]
        self.TIME = indbRow[2]
        self.S1 = indbRow[25]
        self.S2 = indbRow[26]
        self.S3 = indbRow[27]
        self.B1 = indbRow[35]
        self.B2 = indbRow[36]
        self.B3 = indbRow[37]
        self.B1V = indbRow[45]
        self.B2V = indbRow[46]
        self.B3V = indbRow[47]
        self.S1V = indbRow[55]
        self.S2V = indbRow[56]
        self.S3V = indbRow[57]


class rowsForNTime:

    def __init__(self, inrowLength):
        # rows[0]表示当前时刻数据
        rowLength = inrowLength
        rows = rowForATime[rowLength]
    def addRow(rowForATime):
        for i in range(0, rowLength):
            rows[i + 1] = rows[i]
        rows[0] = rowsForNTime


# 连接数据库
connect = pymysql.Connect(
    host='www.qtmm.info',
    port=3306,
    user='qtadmin',
    passwd='admin558',
    db='qt_schema',
    charset='utf8'
)

# 获取游标
cursor = connect.cursor()

# 查询数据
sqlstr = "SELECT * from qt_schema.qt_wsqauto order by RT_CODE,RT_DATE,RT_TIME"
cursor.execute(sqlstr)
while True:
    dbRow = cursor.fetchone()
    # 初始化数据  吧数据一行一行插进去，超过一定量20就换出
    rowForATime.__init__(dbRow)
    rowsForNTime.addRow(rowForATime)
    # 判断条件
    '''
    1）T+1买1价上涨
    2）T+1买1价不变  and    T+1卖1量<T卖1量 （说明卖单被吃）
      说明卖单被吃掉 有上涨动力
    3）T+1买1价不变  and    T+1买1量>T买1量 （说明狂挂买单了）
    4）T+1买1价不变  and    T+1卖1价上涨，说明出现空挡
    满足以下任一条件即为“满足放量”
    5）T+1成交量》T成交量的n倍
    '''
    # 条件1
    # if #
    # factor1 = True
    # 条件2
    # 总条件判断

# 运行策略2
# myStrategy2()

# 关闭连接
cursor.close()
connect.close()
