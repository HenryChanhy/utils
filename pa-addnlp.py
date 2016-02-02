#! /usr/bin/env python2.7
#-*- coding: utf8 -*-
import csv, codecs, cStringIO
from snownlp import SnowNLP
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("input_csv")
parser.add_argument("-H","--history")
parser.add_argument("-o","--output_csv")
parser.add_argument("-d","--dup_csv")
parser.add_argument("-m","--mis_csv")
args = parser.parse_args()
namesplit= args.input_csv.rsplit(".")[0]
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
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class GBKWriter:
    def __init__(self, f, dialect=csv.excel, encoding="gbk", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()
    def writerow(self, row):
        self.writer.writerow([s.encode("gbk") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("gbk")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

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


#f =open("pa-87w.csv",'r')
#allLines=f.readlines()
zhixia=(u'北京',u'天津',u'上海',u'重庆')
zhixiashi=(u'北京市',u'天津市',u'上海市',u'重庆市')
zizhi=(u'内蒙古',u'西藏')
provinces=(u'河北',u'山西',u'辽宁',u'吉林',u'黑龙江',u'江苏',
    u'浙江',u'安徽',u'福建',u'江西',u'山东',u'河南',u'湖北',u'湖南',u'广东',
    u'海南',u'四川',u'贵州',u'云南',u'陕西',u'甘肃',u'青海',u'宁夏')
provincesf=(u'北京市',u'天津市',u'河北省',u'山西省',u'内蒙古自治区',u'辽宁省',u'吉林省',u'黑龙江省',u'上海市',u'江苏省',
    u'浙江省',u'安徽省',u'福建省',u'江西省',u'山东省',u'河南省',u'湖北省',u'湖南省',u'广东省',u'广西壮族自治区',
    u'海南省',u'重庆市',u'四川省',u'贵州省',u'云南省',u'西藏自治区',u'陕西省',u'甘肃省',u'青海省',u'宁夏回族自治区',u"新疆维吾尔自治区"
)

citys=[]
dictcity={}
with open("citys.txt", 'rb') as f:
    #reader = csv.reader(f)
    reader = UnicodeReader(f)
#    headers = reader.next()
    for row in reader:
        citys.append(row[1])
        dictcity[row[1]]=row[0]
f.close()

def findprovincebycity(straddr):
    if straddr in dictcity:
        return getprovince(dictcity[straddr])
    elif straddr+u"市" in dictcity:
        return getprovince(dictcity[straddr+u"市"])
    else:
        return None

def getprovince(straddr):
    if straddr in provinces:
        result=straddr+u"省"
    elif straddr in provincesf:
        result=straddr
    elif straddr in zhixia:
        result=straddr+u"市"
    elif straddr in zizhi:
        result=straddr+u"自治区"
    elif straddr == u"广西":
        result=u"广西壮族自治区"
    elif straddr == u"宁夏":
        result=u"宁夏回族自治区"
    elif straddr == u"新疆":
        result=u"新疆维吾尔自治区"
    else:
        result=None
    return result

phone87w=[]
dict87w={}


#for i in range(1,len(allLines)):
#    phone87w.append(allLines[i].split(",")[0])
#    dict87w[allLines[i].split(",")[0]]=i

with open(args.input_csv, 'rb') as f:
    #reader = csv.reader(f)
    reader = UnicodeReader(f)
    headers = reader.next()
    for row in reader:
        phone87w.append(row[0])
        dict87w[row[0]]=row
phone=[]
for i in open(args.history):
    phone.append(i[0:11])

sphone=set(phone)
cs=set(phone87w) & sphone
vs=set(phone87w) -cs

print len(phone87w)
print len(vs)
print len(cs)

true87w=open(args.output_csv,"w")

#true87=csv.writer(true87w)
true87=UnicodeWriter(true87w)
misaddrw=open(args.mis_csv,"w")
misaddr=UnicodeWriter(misaddrw)
wrong87w=open(args.dup_csv,"w")
#wrong87=csv.writer(wrong87w)
wrong87=UnicodeWriter(wrong87w)
wrong87.writerow(headers)
for c in cs:
    wrong87.writerow(dict87w[c])
wrong87w.close()

true87.writerow(headers)
for v in vs:
    content=dict87w[v]
    address_status=True
    for contents in content[7:]:
        content[6]+=contents
    if content[6]!='' and content[4]!='': #有详细地址字段
        addr=getprovince(content[3])
        if addr!=None:
            content[3]=addr
        else: #省信息不对
            address_status=False
        s=SnowNLP(content[6])
        addrs=s.words
        if len(addrs) > 2:
            if addrs[0] in provincesf:
                #过滤重复的省之间内容
                i=1
                if addrs[0] in addrs[1:]:
                    for i in range(1,len(addrs)):
                        if addrs[0] == addrs[i]:
                            content[6]=u""
                            break
                    for addr in addrs[i:]:
                        if not content[6].endswith(c):
                            content[6]+=c
                #过滤重复的市之间内容
                if i < len(addrs)-2:
                    if addrs[i] in addrs[i+1:]:
                        for i2 in range(i+1,len(addrs)):
                            if addrs[i] == addrs[i2]:
                                break
                        for addr in addrs[i+1:]:
                            if not content[6].endswith(c):
                                content[6]+=c
            elif addrs[0] in zhixia:
                content[6]=addrs[0]+u"市"
                for c in addrs[1:]:
                    if not content[6].endswith(c):
                        content[6]+=c

            elif addrs[0] in zizhi:
                content[6]=addrs[0]+u"自治区"
                for c in addrs[1:]:
                    if not content[6].endswith(c):
                        content[6]+=c
            elif addrs[0] == u"新疆":
                content[6]=addrs[0]+u"维吾尔自治区"
                for c in addrs[1:]:
                    if not content[6].endswith(c):
                        content[6]+=c
            elif addrs[0] == u"广西":
                content[6]=u"广西壮族自治区"
                for c in addrs[1:]:
                    if not content[6].endswith(c):
                        content[6]+=c
            elif addrs[0] == u"宁夏":
                content[6]=u"宁夏回族自治区"
                for c in addrs[1:]:
                    if not content[6].endswith(c):
                        content[6]+=c
            elif addrs[0] in provinces:
                content[6]=addrs[0]+u"省"
                for c in addrs[1:]:
                    if not content[6].endswith(c):
                        content[6]+=c
            else:
                if addr in zhixiashi:
                    addrfull=content[3]
                    for contents in content[5:]:
                        addrfull+=contents
                    content[6]=addrfull
                else:
                    addrfull=content[3]
                    for contents in content[4:]:
                        addrfull+=contents
                    content[6]=addrfull
        else:#內容過短
            address_status=False
    else:#没有详细地址字段，需拆分省市区
        fulladdr=addr=content[3]
        content[6]=content[3]
        if content[6]!=u"":
            s=SnowNLP(content[6])
            addrs=s.words
            if len(addrs) > 3:
                addr=getprovince(addrs[0])
                if addr != None:
                    content[3]=addr


                #过滤重复的省之间内容
                    i=0
                    if addrs[0] in addrs[1:]:
                        i=1
                        for i in range(1,len(addrs)):
                            if addrs[0] == addrs[i]:
                                content[6]=u""
                                break
                        for addr in addrs[i:]:
                            if not content[6].endswith(addr):
                                content[6]+=addr


                    i1=i
                    i2=0
                    i3=0
                    content[4]=u""
                    for i2 in range(i1+1,len(addrs)):
                        content[4]+=addrs[i2]
                        if addrs[i2].endswith(u"市") or addrs[i2].endswith(u"自治州") or addrs[i2].endswith(u"区") or addrs[i2].endswith(u"县"):
                            break
                    #过滤重复的市之间内容
                    if addrs[i2] in addrs[i2+1:]:
                        for i3 in range(i2+1,len(addrs)):
                            if addrs[i2] == addrs[i3]:
                                content[6]=content[3]
                                break
                        for addr in addrs[i3:]:
                            if not content[6].endswith(addr):
                                content[6]+=addr
                    else:
                        i3+=1

                    if i3 < len(addrs)-2:
                        content[5]=addrs[i3+1]
                    else:
                        address_status=False

                else:
                    addr=findprovincebycity(addrs[0])
                    if addr != None:
                        content[3]=addr
                        content[6]=addr
                        content[4]=u""
                        i2=i3=0
                        for i2 in range(0,len(addrs)):
                            content[4]+=addrs[i2]
                            if not content[6].endswith(addrs[i2]):
                                content[6]+=addrs[i2]
                            if (not (u"市" in fulladdr or u"自治州" in fulladdr) ) or addrs[i2].endswith(u"市") or addrs[i2].endswith(u"自治州") or addrs[i2].endswith(u"区") or addrs[i2].endswith(u"县"):
                                break
                        #过滤重复的市之间内容
                        if addrs[i2] in addrs[i2+1:]:
                            for i3 in range(i2+1,len(addrs)):
                                if addrs[i2] == addrs[i3]:
                                    content[6]=content[3]
                                    break
                            for addr in addrs[i3:]:
                                if not content[6].endswith(addr):
                                    content[6]+=addr

                        if i3 < len(addrs)-2:
                            content[5]=addrs[i3+1]
                            for addr in addrs[i3+1:]:
                                if not content[6].endswith(addr):
                                    content[6]+=addr
                        else:
                            address_status=False

                    else:#no province in address
                        address_status=False
            else:
                address_status=False
#            print content
    if address_status :
        true87.writerow(content[:8])
    else:
        misaddr.writerow(content)
true87w.close()
misaddrw.close()
































