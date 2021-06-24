import tushare as ts
import time
import numpy as np
import pandas as pd

buylist = []
selllist = []

time_ys = 2.8
vol = 200

# codes=ts.get_hs300s().code

codes = ['300059', '600111', '601066', '600446', '600887', '600516', '600104', '000413', '600352', '603000', '000713',
         '300498', '600031', '600585', '002405', '002230', '600118', '000977', '600050', '300070', '000636', '600150',
         '600848', '601111', '600489', '601225', '600760', '000807', '002460', '000070', '600406', '600460']
# codef=pd.DataFrame(codes)
# codef.to_csv('C:\\example\\code.csv',index=1,header=1)
df = ts.get_realtime_quotes(codes)

W = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
W = W.replace('', 0)
time.sleep(time_ys)

df = ts.get_realtime_quotes(codes)
X = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
X = X.replace('', 0)
time.sleep(time_ys)

df = ts.get_realtime_quotes(codes)
Y = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
Y = Y.replace('', 0)
time.sleep(time_ys)
df = ts.get_realtime_quotes(codes)
Z = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
Z = Z.replace('', 0)
time.sleep(time_ys)

df = ts.get_realtime_quotes(codes)
A = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
A = A.replace('', 0)

time.sleep(time_ys)
df = ts.get_realtime_quotes(codes)

B = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
B = B.replace('', 0)

time.sleep(time_ys)
df = ts.get_realtime_quotes(codes)
C = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
C = C.replace('', 0)
time.sleep(time_ys)
df = ts.get_realtime_quotes(codes)
D = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
D = D.replace('', 0)
time.sleep(time_ys)
df = ts.get_realtime_quotes(codes)
E = df[['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
        'volume']]
E = E.replace('', 0)

for n in range(0, 600):
    W = X
    X = Y
    Y = Z
    Z = A
    A = B
    B = C
    C = D
    D = E

    time.sleep(time_ys)
    df = ts.get_realtime_quotes(codes)

    E = df[
        ['code', 'a1_p', 'a2_p', 'a3_p', 'b1_p', 'b2_p', 'b3_p', 'b1_v', 'b2_v', 'b3_v', 'a1_v', 'a2_v', 'a3_v', 'time',
         'volume']]
    E = E.replace('', 0)

    for i in range(0, 31):
        #买一价不变，买一量减少
        bflag1 = ((E.iloc[i, 4] == D.iloc[i, 4] and float(E.iloc[i, 10]) + vol < float(D.iloc[i, 10]) and float(
            E.iloc[i, 7]) > float(D.iloc[i, 7]) + vol)
                  or (E.iloc[i, 4] > D.iloc[i, 4] and float(E.iloc[i, 7]) > float(D.iloc[i, 7]) + vol) or (
                              E.iloc[i, 4] > D.iloc[i, 4] and float(E.iloc[i, 8]) > float(D.iloc[i, 8]) + vol)
                  or (E.iloc[i, 4] > D.iloc[i, 4] and float(E.iloc[i, 10]) + vol < float(D.iloc[i, 10])) or (
                              E.iloc[i, 4] == D.iloc[i, 4] and float(D.iloc[i, 1]) < float(E.iloc[i, 1]))
                  or (E.iloc[i, 4] > D.iloc[i, 4] and float(E.iloc[i, 7]) < float(D.iloc[i, 7]) and float(
                    D.iloc[i, 11]) - vol > float(E.iloc[i, 10])))

        bflag2 = ((D.iloc[i, 4] == C.iloc[i, 4] and float(D.iloc[i, 10]) + vol < float(C.iloc[i, 10]) and float(
            D.iloc[i, 7]) > float(C.iloc[i, 7]) + vol)
                  or (D.iloc[i, 4] > C.iloc[i, 4] and float(D.iloc[i, 7]) > float(C.iloc[i, 7]) + vol) or (
                              D.iloc[i, 4] > C.iloc[i, 4] and float(D.iloc[i, 8]) > float(C.iloc[i, 8]) + vol)
                  or (D.iloc[i, 4] > C.iloc[i, 4] and float(D.iloc[i, 10]) + vol < float(C.iloc[i, 10])) or (
                              D.iloc[i, 4] == C.iloc[i, 4] and float(C.iloc[i, 1]) < float(D.iloc[i, 1]))
                  or (D.iloc[i, 4] > C.iloc[i, 4] and float(D.iloc[i, 7]) < float(C.iloc[i, 7]) and float(
                    C.iloc[i, 11]) - vol > float(D.iloc[i, 10])))

        bflag3 = ((C.iloc[i, 4] == B.iloc[i, 4] and float(C.iloc[i, 10]) + vol < float(B.iloc[i, 10]) and float(
            C.iloc[i, 7]) > float(B.iloc[i, 7]) + vol)
                  or (C.iloc[i, 4] > B.iloc[i, 4] and float(C.iloc[i, 7]) > float(B.iloc[i, 7]) + vol) or (
                              C.iloc[i, 4] > B.iloc[i, 4] and float(C.iloc[i, 8]) > float(B.iloc[i, 8]) + vol)
                  or (C.iloc[i, 4] > B.iloc[i, 4] and float(C.iloc[i, 10]) + vol < float(B.iloc[i, 10])) or (
                              C.iloc[i, 4] == B.iloc[i, 4] and float(B.iloc[i, 1]) < float(C.iloc[i, 1]))
                  or (C.iloc[i, 4] > B.iloc[i, 4] and float(C.iloc[i, 7]) < float(B.iloc[i, 7]) and float(
                    B.iloc[i, 11]) - vol > float(C.iloc[i, 10])))

        bflag4 = ((B.iloc[i, 4] == A.iloc[i, 4] and float(B.iloc[i, 10]) + vol < float(A.iloc[i, 10]) and float(
            B.iloc[i, 7]) > float(A.iloc[i, 7]) + vol)
                  or (B.iloc[i, 4] > A.iloc[i, 4] and float(B.iloc[i, 7]) > float(A.iloc[i, 7]) + vol) or (
                              B.iloc[i, 4] > A.iloc[i, 4] and float(B.iloc[i, 8]) > float(A.iloc[i, 8]) + vol)
                  or (B.iloc[i, 4] > A.iloc[i, 4] and float(B.iloc[i, 10]) + vol < float(A.iloc[i, 10])) or (
                              B.iloc[i, 4] == A.iloc[i, 4] and float(A.iloc[i, 1]) < float(B.iloc[i, 1]))
                  or (B.iloc[i, 4] > A.iloc[i, 4] and float(B.iloc[i, 7]) < float(A.iloc[i, 7]) and float(
                    A.iloc[i, 11]) - vol > float(B.iloc[i, 10])))

        bflag5 = (float(E.iloc[i, 14]) - float(A.iloc[i, 14])) / (float(A.iloc[i, 14]) - float(W.iloc[i, 14]) + 1)

        # sflag1=(E.iloc[i,1]==D.iloc[i,1] and E.iloc[i,7]<D.iloc[i,7]) or (E.iloc[i,1]==D.iloc[i,1] and E.iloc[i,10]>D.iloc[i,10]) or (E.iloc[i,1]<D.iloc[i,1])

        # sflag2=(D.iloc[i,1]==C.iloc[i,1] and D.iloc[i,7]<C.iloc[i,7]) or (D.iloc[i,1]==C.iloc[i,1] and D.iloc[i,10]>C.iloc[i,10]) or (D.iloc[i,1]<C.iloc[i,1])

        # sflag3=(C.iloc[i,1]==B.iloc[i,1] and C.iloc[i,7]<B.iloc[i,7]) or (C.iloc[i,1]==B.iloc[i,1] and C.iloc[i,10]>B.iloc[i,10]) or (C.iloc[i,1]<B.iloc[i,1])
        if bflag1 and bflag2 and bflag3 and bflag5 > 4:
            buylist.append(E.iloc[i, 0])
            buylist.append(E.iloc[i, 13])
            buylist.append(E.iloc[i, 1])
