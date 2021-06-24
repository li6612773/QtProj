
import pandas as pd


from WindPy import *
from datetime import datetime, date, timedelta
import time

from sqlalchemy import create_engine

connect_info = 'mysql+pymysql://qtadmin:admin558@localhost:3306/qt_schema?charset=utf8'
engine = create_engine(connect_info) #use sqlalchemy to build link-engine



w.start();
w.isconnected()


buylist = []
buytable = []

t = w.wsq("600000.SH,600004.SH,600006.SH,600007.SH,600008.SH,600009.SH,600010.SH,600011.SH,600012.SH,600015.SH",
          "rt_pct_chg_1min,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,rt_asize1,rt_asize2,rt_asize3")




t.Fields.append("time")
t.Data.append(t.Times * 10)

rr = pd.DataFrame(t.Data, index=t.Fields, columns=t.Codes)

tosqlret=rr.to_sql('qt_test',engine,chunksize=1000,if_exists='replace',index=False)
print(tosqlret)

rr.T
A = rr.T

time.sleep(2)
t = w.wsq("600000.SH,600004.SH,600006.SH,600007.SH,600008.SH,600009.SH,600010.SH,600011.SH,600012.SH,600015.SH",
          "rt_pct_chg_1min,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,rt_asize1,rt_asize2,rt_asize3")
t.Fields.append("time")
t.Data.append(t.Times * 10)
rr = pd.DataFrame(t.Data, index=t.Fields, columns=t.Codes)
B = rr.T

time.sleep(2)
t = w.wsq("600000.SH,600004.SH,600006.SH,600007.SH,600008.SH,600009.SH,600010.SH,600011.SH,600012.SH,600015.SH",
          "rt_pct_chg_1min,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,rt_asize1,rt_asize2,rt_asize3")
t.Fields.append("time")
t.Data.append(t.Times * 10)
rr = pd.DataFrame(t.Data, index=t.Fields, columns=t.Codes)
C = rr.T

time.sleep(2)
t = w.wsq("600000.SH,600004.SH,600006.SH,600007.SH,600008.SH,600009.SH,600010.SH,600011.SH,600012.SH,600015.SH",
          "rt_pct_chg_1min,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,rt_asize1,rt_asize2,rt_asize3")
t.Fields.append("time")
t.Data.append(t.Times * 10)
rr = pd.DataFrame(t.Data, index=t.Fields, columns=t.Codes)
D = rr.T

time.sleep(2)
t = w.wsq("600000.SH,600004.SH,600006.SH,600007.SH,600008.SH,600009.SH,600010.SH,600011.SH,600012.SH,600015.SH",
          "rt_pct_chg_1min,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,rt_asize1,rt_asize2,rt_asize3")
t.Fields.append("time")
t.Data.append(t.Times * 10)
rr = pd.DataFrame(t.Data, index=t.Fields, columns=t.Codes)
E = rr.T
"""
for n in range(1, 200):
    A = B
    B = C
    C = D
    D = E
"""
time.sleep(2)
t = w.wsq("600000.SH,600004.SH,600006.SH,600007.SH,600008.SH,600009.SH,600010.SH,600011.SH,600012.SH,600015.SH",
              "rt_pct_chg_1min,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,rt_asize1,rt_asize2,rt_asize3")
t.Fields.append("time")
t.Data.append(t.Times * 10)
rr = pd.DataFrame(t.Data, index=t.Fields, columns=t.Codes)
E = rr.T

"""
#for i in range(0, 10):
    bflag1 = ((E.iloc[i, 4] == D.iloc[i, 4] and E.iloc[i, 10] < D.iloc[i, 10] and E.iloc[i, 7] > D.iloc[i, 7])
              or (E.iloc[i, 4] > D.iloc[i, 4] and E.iloc[i, 7] > D.iloc[i, 7]) or (
                          E.iloc[i, 4] > D.iloc[i, 4] and E.iloc[i, 8] > D.iloc[i, 8])
              or (E.iloc[i, 4] > D.iloc[i, 4] and E.iloc[i, 10] < D.iloc[i, 10]) or (
                          E.iloc[i, 4] == D.iloc[i, 4] and D.iloc[i, 1] < E.iloc[i, 1])
              or (E.iloc[i, 4] > D.iloc[i, 4] and E.iloc[i, 7] < D.iloc[i, 7] and D.iloc[i, 11] < E.iloc[i, 10]))

    bflag2 = ((D.iloc[i, 4] == C.iloc[i, 4] and D.iloc[i, 10] < C.iloc[i, 10] and D.iloc[i, 7] > C.iloc[i, 7])
              or (D.iloc[i, 4] > C.iloc[i, 4] and D.iloc[i, 7] > C.iloc[i, 7]) or (
                          D.iloc[i, 4] > C.iloc[i, 4] and D.iloc[i, 8] > C.iloc[i, 8])
              or (D.iloc[i, 4] > C.iloc[i, 4] and D.iloc[i, 10] < C.iloc[i, 10]) or (
                          D.iloc[i, 4] == C.iloc[i, 4] and C.iloc[i, 1] < D.iloc[i, 1])
              or (D.iloc[i, 4] > C.iloc[i, 4] and D.iloc[i, 7] < C.iloc[i, 7] and C.iloc[i, 11] < D.iloc[i, 10]))

    bflag3 = ((C.iloc[i, 4] == B.iloc[i, 4] and C.iloc[i, 10] < B.iloc[i, 10] and C.iloc[i, 7] > B.iloc[i, 7])
              or (C.iloc[i, 4] > B.iloc[i, 4] and C.iloc[i, 7] > B.iloc[i, 7]) or (
                          C.iloc[i, 4] > B.iloc[i, 4] and C.iloc[i, 8] > B.iloc[i, 8])
              or (C.iloc[i, 4] > B.iloc[i, 4] and C.iloc[i, 10] < B.iloc[i, 10]) or (
                          C.iloc[i, 4] == B.iloc[i, 4] and B.iloc[i, 1] < C.iloc[i, 1])
              or (C.iloc[i, 4] > B.iloc[i, 4] and C.iloc[i, 7] < B.iloc[i, 7] and B.iloc[i, 11] < C.iloc[i, 10]))

    bflag4 = ((B.iloc[i, 4] == A.iloc[i, 4] and B.iloc[i, 10] < A.iloc[i, 10] and B.iloc[i, 7] > A.iloc[i, 7])
              or (B.iloc[i, 4] > A.iloc[i, 4] and B.iloc[i, 7] > A.iloc[i, 7]) or (
                          B.iloc[i, 4] > A.iloc[i, 4] and B.iloc[i, 8] > A.iloc[i, 8])
              or (B.iloc[i, 4] > A.iloc[i, 4] and B.iloc[i, 10] < A.iloc[i, 10]) or (
                          B.iloc[i, 4] == A.iloc[i, 4] and A.iloc[i, 1] < B.iloc[i, 1])
              or (B.iloc[i, 4] > A.iloc[i, 4] and B.iloc[i, 7] < A.iloc[i, 7] and A.iloc[i, 11] < B.iloc[i, 10]))

    bflag5 = (E.iloc[i, 0] > 0.01)
"""
#  if bflag1 and bflag2 and bflag3 and bflag4 and bflag5:
i=1
buylist.append(E.index[i])
buylist.append(E.iloc[i, 13])
buylist.append(E.iloc[i, 1])
buytable.append(buylist)

i=2

