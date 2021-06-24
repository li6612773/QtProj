import tushare as ts
import pandas as pd
import datetime
f=open('D:\stocks.txt')
time1=datetime.datetime.now()
stocks=[line.strip() for line in f.readlines()]
data1=ts.get_realtime_quotes(stocks[0:880])
data2=ts.get_realtime_quotes(stocks[880:1760])
data3=ts.get_realtime_quotes(stocks[1760:2640])
data4=ts.get_realtime_quotes(stocks[2640:-1])
time2=datetime.datetime.now()
print('开始时间：'+str(time1))
print('结束时间：'+str(time2))
print(data1)
print(data2)
print(data3)
print(data4)
