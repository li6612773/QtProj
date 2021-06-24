# 主程序
from WindPy import *

w.start();
w.isconnected()
import pandas as pd
import time

t = w.wsq("600000.SH,600004.SH,600006.SH,600007.SH,600008.SH,600009.SH,600010.SH,600011.SH,600012.SH,600015.SH",
          "rt_pct_chg_1min,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,rt_asize1,rt_asize2,rt_asize3")

t.Fields.append("time")
t.Data.append(t.Times * 10)

rr = pd.DataFrame(t.Data, index=t.Fields, columns=t.Codes)

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
for n in range(1, 2):
    A = B
    B = C
    C = D
    D = E

    time.sleep(2)
    t = w.wsq("600000.SH,600004.SH,600006.SH,600007.SH,600008.SH,600009.SH,600010.SH,600011.SH,600012.SH,600015.SH",
              "rt_pct_chg_1min,rt_ask1,rt_ask2,rt_ask3,rt_bid1,rt_bid2,rt_bid3,rt_bsize1,rt_bsize2,rt_bsize3,rt_asize1,rt_asize2,rt_asize3")
    t.Fields.append("time")
    t.Data.append(t.Times * 10)
    rr = pd.DataFrame(t.Data, index=t.Fields, columns=t.Codes)
    E = rr.T

    for n in range(0, 10):
((E.iloc[i, 4] == D.iloc[i, 4] and E.iloc[i, 10] < D.iloc[i, 10] and E.iloc[i, 7] > D.iloc[i, 7])
 or (E.iloc[i, 4] > D.iloc[i, 4] and E.iloc[i, 7] > D.iloc[i, 7]))
"""













