""""
Dim w_wsq_data As Variant
Dim w_wsq_codes As Variant, w_wsq_fields As Variant, w_wsq_times As Variant
Dim w_wsq_errorid As Long
"""

import threading
from WindPy import w
w.start();

#define the callback function
def myCallback(indata):
    if indata.ErrorCode!=0:
        print('error code:'+str(indata.ErrorCode)+'\n');
        return();

    global begintime
    lastvalue ="";
    for k in range(0,len(indata.Fields)):
         if(indata.Fields[k] == "RT_LAST"):
            lastvalue = str(indata.Data[k][0]);

    string =  lastvalue +"\n";
    print(string);

        w.start()
        w.wsq("IF1512.CFE","rt_time,rt_last",func=myCallback)
#to subscribe if14.CFE
print('\n\n'+'-----通过wsq来提取即时行情，采用回调方式-----'+'\n')
#该函数传入回调函数，返回实时数据
w_wsq_data = vba_wsqSubscribe("000001.SZ,000002.SZ","rt_date,rt_time,rt_ask1,rt_asize1,"
                            "rt_ask2,rt_asize2,rt_ask3,rt_asize3,rt_bid1,rt_bsize1,rt_bid2,rt_bsize2,"
                            "rt_bid3,rt_bsize3", AddressOf wsqcallback, w_wsq_errorid)
Public Sub wsqcallback(k, codes, indics, times As Variant, ByVal reqid As Long, ByVal errcode As Long)


#wsddata1=w.wsd("000001.SZ", "open,high,low,close,volume,amt", "2015-11-22", "2015-12-22", "Fill=Previous")
#printpy(wsddata1)
