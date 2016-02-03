#-*- coding: utf8 -*-
__author__ = 'Henry'
import os
import time
import hashlib
import requests
import json
import csv, codecs, cStringIO
import argparse
import re
p=re.compile("[\s+\.\!\/\|_,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）＠]+".decode("utf8"))
paddress=re.compile("-".decode("utf8"))

parser = argparse.ArgumentParser()
parser.add_argument("input_csv")
parser.add_argument("-H","--history")
parser.add_argument("-o","--output_csv")
parser.add_argument("-d","--dup_csv")
parser.add_argument("-m","--mis_csv")
parser.add_argument("-e","--input_err_csv")
args = parser.parse_args()
namesplit= args.input_csv.split(".")[0]
if args.input_err_csv==None:
    args.input_err_csv=namesplit+".err.csv"
if args.history==None:
    args.history="testphone.txt"
if args.output_csv==None:
    args.output_csv=namesplit+'_output.csv'
if args.dup_csv==None:
    args.dup_csv=namesplit+'_dup.csv'
if args.mis_csv==None:
    args.mis_csv=namesplit+'_mis.csv'

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()

        return [p.sub("",unicode(s, "utf-8")) for s in row]

    def __iter__(self):
        return self

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def cmp(x,y):
    x1=x.lower()
    y1=y.lower()
    if x1>y1:
        return 1
    if x1<y1:
        return -1
    return 0

def GetTimeStamp():
    t=time.localtime()
    lst=[]
    for i in xrange(len(t)):
        if i>=5:
            break
        v=str(t[i]).zfill(2)
        lst.append(v)
    reStr="".join(lst)
    return reStr

def log_file(msg):
    dir=os.getcwd()
    fileName="err.txt"
    finalPath=os.path.join(dir,fileName)
    with open(finalPath,"ab") as f:
        f.write("[%s] %s\r\n"%(GetTimeStamp(),msg))
    return finalPath

def MD5Sign(md5Str):
    m=hashlib.md5()
    m.update(md5Str)
    return m.hexdigest().upper()

def TrialAPI(**arg):
    def _TrialAPI(func):
        def __TrialAPI(data):
            SysData={"appKey":"updateandtos",}
            ExData={"apiSecret":"iO7hbmH8-rbt6Hg_Yg6",}
            param=func(data)
            param.update(SysData)
            param.update(data)
            url="https://int.taotonggroup.com/pampers1/TtApi/"+arg["method"]
            param["timestamp"]=str(int(time.time()))
            paraList=[]
            for k,v in param.items():
                if isinstance(v,str):
                    paraList.append(k+"="+str(v))
            paraList.sort(cmp)
            md5Str="&".join(paraList)
            md5Str=md5Str+ExData["apiSecret"]
            param["sig"]=MD5Sign(md5Str)
#            log_file("%s %s"%(md5Str,param["sig"]))
#            jsparam=json.dumps(param)
            req=requests.get(url+"?"+md5Str)
            log_file("%s %s"%(url+"?"+md5Str,req.content))
            return req.content
        return __TrialAPI
    return _TrialAPI

@TrialAPI(method="updateOrderTrackingInfo")
def updateOTI(data):
    return {}

@TrialAPI(method="test_TOS.json")
def updateTOS(data):
    return {}

def test_TOS():
    with open(args.input_csv, 'rb') as f:
        #reader = csv.reader(f)
        reader = UnicodeReader(f)
        headers = reader.next()
        for row in reader:
            data={"out_tid":row[2].encode('utf8')}
            data["status"]="3"
            result=updateTOS(data)
            #log_file("data:%s result:%s"%(data,result))
    f.close()
    with open(args.input_err_csv,'rb') as f:
        reader = UnicodeReader(f)
        headers = reader.next()
        for row in reader:
            data={"out_tid":row[2].encode('utf8')}
            data["status"]="2"
            data["msg"]=row[16].encode('utf8')
            result=updateTOS(data)
            #log_file("data:%s result:%s"%(data,result))
    f.close()


if __name__=="__main__":
    test_TOS()

