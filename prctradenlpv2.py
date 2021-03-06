#! /usr/bin/env python2.7
#-*- coding: utf8 -*-
import csv, codecs, cStringIO
from snownlp import SnowNLP
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

        return [p.sub("",unicode(s, "utf-8")) for s in row]

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
    u'海南',u'四川',u'贵州',u'云南',u'陕西',u'甘肃',u'青海')
provincesf=(u'北京',u'上海',u'天津',u'重庆',u'香港',u'澳门',u'河北省',u'山西省',u'辽宁省',u'吉林省',u'黑龙江省',u'江苏省',
    u'浙江省',u'安徽省',u'福建省',u'江西省',u'山东省',u'河南省',u'湖北省',u'湖南省',u'广东省',u'广西壮族自治区',
    u'海南省',u'四川省',u'贵州省',u'云南省',u'西藏自治区',u'陕西省',u'甘肃省',u'青海省',u'宁夏回族自治区',u"新疆维吾尔自治区",u'内蒙古自治区')
fuzzywords=(u'省',u'市',u'县',u'镇',u'工业区',u'业园区',u'工业园',u'公园',u'管理区',u'开发区',u'新区',u'大道',u'国道',
            u'道',u'路',u'桥',u'路口',u'街上',u'街',u'门口',u'市场',u'附近',u'车站',u'快递',u'物流',u'自取',u'自提',
            u'桥头',u'交叉口',u'三岔口',u'交口')
citys=[]
dictcity={}
PCA={}
areas=[]
with open("citys.csv", 'rb') as f:
    #reader = csv.reader(f)
    reader = UnicodeReader(f)
#    headers = reader.next()
    for row in reader:
        citys.append(row[1])
        dictcity[row[1]]=row[0]
f.close()

with open("pca.csv",'rb') as f:
    reader = UnicodeReader(f)
    for row in reader:
        areas.append(row[3])
        if not PCA.has_key(row[4]):
            PCA[row[4]]={}
        PCA[row[4]][row[3]]=row[5]
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
        result=straddr
    elif straddr in zhixiashi:
        result=straddr[:2]
    elif straddr in zizhi:
        result=straddr+u"自治区"
    elif straddr.startswith(u"广西"):
        result=u"广西壮族自治区"
    elif straddr.startswith(u"宁夏"):
        result=u"宁夏回族自治区"
    elif straddr.startswith(u"新疆"):
        result=u"新疆维吾尔自治区"
    else:
        result=None
    return result

def getcity(straddr):
    if straddr == u"毕节市" or straddr == u"毕节":
        return u"毕节地区"
    elif straddr == u"铜仁市" or straddr == u"铜仁":
        return u"铜仁地区"
    elif straddr == u"市辖区" or straddr ==u"县" or straddr.endswith(u"直辖县级行政区划"):
        return u""
    elif straddr in citys:
        return straddr
    elif straddr+u"市" in citys:
        return straddr +u"市"
    else:
        return None

def getarea(straddr):
    if straddr in areas :
        return straddr
    elif straddr + u"市" in areas:
        return straddr + u"市"
    elif straddr + u"区" in areas:
        return straddr + u"区"
    elif straddr + u"县" in areas:
        return straddr + u"县"
    else:
        return None

def getcitybyProvArea(prov,straddr):
    if PCA[prov].has_key(straddr):
        return PCA[prov][straddr]
    elif PCA[prov].has_key(straddr+u"区"):
        return PCA[prov][straddr+u"区"]
    else:
        return None

def filterdupaddress(straddr):
    dup=0
    for dup in range(len(straddr)/2,2,-1):
        if straddr[:dup] in straddr[dup:]:
            break
    if dup > 3:
#        dupsplit=straddr.rsplit(straddr[:dup-1],1)
        return straddr[dup:]
    else:
        return straddr

def dynareduce(address):
    addwords=SnowNLP(address).words
    for pos in range(0,len(addwords)-1):
        for pos1 in range(pos+1,len(addwords)-1):
            if addwords[pos] == addwords[pos1]:
                addwords[pos1]=u""
    result=u""
    for words in addwords:
        result+=words
    return result

def filterotherarea(straddr):
    if u"其它区" in straddr:
#        addrsplit = straddr.split(u"其它区",1)
        return straddr.replace(u"其它区",u"")
    elif u"市辖区" in straddr:
        return straddr.replace(u"市辖区",u"")
    elif u"璧山区" in straddr:
        return straddr.replace(u"璧山区",u"璧山县")
    elif u"高陵区" in straddr:
        return straddr.replace(u"高陵区",u"高陵县")
    elif u"富阳区" in straddr:
        return straddr.replace(u"富阳区",u"富阳市")
    elif u"藁城区" in straddr:
        return straddr.replace(u"藁城区",u"藁城市")
    elif u"溧水区" in straddr:
        return straddr.replace(u"溧水区",u"溧水县")
    elif u"南溪区" in straddr:
        return straddr.replace(u"南溪区",u"南溪县")
    elif u"鹿泉区" in straddr:
        return straddr.replace(u"鹿泉区",u"鹿泉市")
    elif u"大足区" in straddr:
        return straddr.replace(u"大足区",u"大足县")
    elif u"毕节市" in straddr:
        return straddr.replace(u"毕节市",u"七星关区")
    elif u"铜仁市" in straddr:
        return straddr.replace(u"毕节市",u"铜仁地区")
    else:
        return straddr

def isfuzzyend(straddr):
    for i in fuzzywords:
        if straddr.endswith(i):
            return True
    return False

def procaddress(content):
     if len(content)>7:
        if content[3]==u"-":
            content[3]=u""
            content[4]=u""
            content[5]=u""
            content[6]=paddress.sub("",content[6])
        content.append(content[6])
        Detailaddress=filterotherarea(dynareduce(content[6]))
        content[6]=Detailaddress
        content.append(u"")
    else:
        print content
        content.append(u"数据不完整")
        return content
    address_status=True
    if content[6]!='' and content[4]!='' and content[5]!='': #有详细地址字段
        addr=getprovince(content[3])
        city=getcity(content[4])
        area=getarea(content[5])
        if addr!=None and city!=None and area!=None:
            content[3]=addr
            area=filterotherarea(area)
            if city == u"":
                city = area
                content[4]=area
            else:
                content[4]=city
            if addr in zhixia:
                content[3]=addr
                content[4]=addr+u"市"
        else: #省或市信息不对
#            address_status=False
            content[13]=u"省市区信息不对"



    return content

def procaddress.o(content):
    if len(content)>7:
        if content[3]==u"-":
            content[3]=u""
            content[4]=u""
            content[5]=u""
            content[6]=paddress.sub("",content[6])
        content.append(content[6])
        Detailaddress=filterotherarea(dynareduce(content[6]))
        content[6]=Detailaddress
        content.append(u"")
    else:
#        print content
        content.append(u"数据不完整")
        return content
    address_status=True
#   详细地址字段组合
#    for contents in content[7:]:
#        content[6]+=contents
    if content[6]!='' and content[4]!='' and content[5]!='': #有详细地址字段
        addr=getprovince(content[3])
        city=getcity(content[4])
        area=getarea(content[5])
        if addr!=None and city!=None and area!=None:
            content[3]=addr
            area=filterotherarea(area)
            if city == u"":
                city = area
                content[4]=area
            else:
                content[4]=city
            if addr in zhixia:
                content[3]=addr
                content[4]=addr+u"市"
        else: #省或市信息不对
#            address_status=False
            content[13]=u"省市区信息不对"
        s=SnowNLP(content[6])
        addrs=s.words
        if len(addrs) > 1:
            if addrs[0] in provincesf:
                i=0
            elif addrs[0] in zhixia:
                content[6]=addrs[0]+u"市"
                for c in addrs[1:]:
                    if not content[6].endswith(c):
                        content[6]+=c

            elif addrs[0] in zizhi:
                content[6]=addrs[0]+u"自治区"
                pos=1
                if addrs[pos] == u"省":
                    pos+=1
                for c in addrs[pos:]:
                    if not content[6].endswith(c):
                        content[6]+=c
            elif addrs[0] == u"新疆":
                content[6]=u"新疆维吾尔自治区"
                pos=1
                if addrs[pos] == u"省":
                    pos+=1
                for c in addrs[pos:]:
                    if not content[6].endswith(c):
                        content[6]+=c
            elif addrs[0] == u"广西":
                content[6]=u"广西壮族自治区"
                pos=1
                if addrs[pos] == u"省":
                    pos+=1
                for c in addrs[pos:]:
                    if not content[6].endswith(c):
                        content[6]+=c
            elif addrs[0] == u"宁夏":
                content[6]=u"宁夏回族自治区"
                pos=1
                if addrs[pos] == u"省":
                    pos+=1
                for c in addrs[pos:]:
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
                    content[5] = area
#                        content[5] = u""
                    for contents in content[5:7]:
                        addrfull+=contents
                    content[6]=addrfull
                else:
                    addrfull=content[3]
#                    if content[5] == u"其它区" or content[5]==u"市辖区":
                    content[5] = area
                    for contents in content[4:7]:
                        addrfull+=contents
                    content[6]=addrfull
#            content[5] == area:

        else:#內容過短
#            address_status=False
            content[13]=u"详细地址太短"
    else:#没有详细地址字段，需拆分省市区
        fulladdr=u""
#        for contents in content[3:]:
        fulladdr+=content[6]
        content[3]=fulladdr
#        content[6]=content[3]
        if content[3]!=u"":
            s=SnowNLP(content[3])
            addrs=s.words
            if len(addrs) > 3:
                addr=getprovince(addrs[0])
                pos=0
                if addr != None:
                    content[3]=addr
                    content[6]=addr
                    pos+=1

                else:#no province in address
                    addr=findprovincebycity(addrs[0])
                    if addr != None:
                        content[3]=addr
                        content[6]=addr
                        content[13]=u"通过市信息补全省份"
#                    else:#no city in address
#                        address_status=False
                if addrs[pos]==u"省":
                        pos+=1
                city=getcity(addrs[pos])
                if content[3] in zhixia:
                    content[4]=content[3]+u"市"
                    if addrs[pos]==u"市辖区" or addrs[pos]==u"县":
                       pos+=1

                    content[6]+=u"市"

                elif city!=None :
                    content[4]=city
                    content[6]+=city
                    pos+=1

                else:

                    if addr != None:
                        citybA=getcitybyProvArea(addr,addrs[pos])
                        if citybA != None:
                            content[4]=citybA
                            content[6]+=citybA
                            content[13]=u"通过地区和省信息补全市信息"

                area=getarea(addrs[pos])
                if area != None and area != u"市辖区":
                    content[5]=area
                    content[6]+=area
                else:
                    content[5]=u""
                    content[6]+=addrs[pos]
#                       address_status=False
                    content[13]=u"找不到区信息"
                pos+=1

#                content[5]=addrs[pos]
#                content[6]+=content[5]
#                pos+=1

                for contents in addrs[pos:]:
                    if not content[6].endswith(contents):
                        content[6]+=contents
            else:# address to short
#                address_status=False
                content[13]=u"详细地址太短"
#            print content
    Detailaddress=filterotherarea(filterdupaddress(content[6]))
    content[6]=Detailaddress
    if isfuzzyend(content[6]):
        content[13]=u"模糊字结尾"
#        address_status = False


    return content



phone8w=[]
phone18w=[]
dict87w={}


#for i in range(1,len(allLines)):
#    phone8w.append(allLines[i].split(",")[0])
#    dict87w[allLines[i].split(",")[0]]=i

with open(args.input_csv, 'rb') as f:
    #reader = csv.reader(f)
    reader = UnicodeReader(f)
    headers = reader.next()
    headers.append("address_backup")
    headers.append("remark")
    for row in reader:
        if len(row)<12:
            print row
        else:
            if u"8片" in row[8]:
                phone8w.append(row[0])
            elif row[11]=="36" or row[11]=="38":
                phone18w.append(row[0])
            else:
                phone8w.append(row[0])
            dict87w[row[0]]=row
#phone8=[]
#for i in open(args.history):
#    phone8.append(i[0:11])
#phone18=[]
#for i in open("ALL18.txt"):
#    phone18.append(i[0:11])
allphones=[]
for i in open(args.history):
    allphones.append(i[0:11])
baby8=[]
for i in open("baby8.txt"):
    baby8.append(i[0:11])

#sphone8=set(phone8)
#sphone18=set(phone18)
#allsets=sphone8 | sphone18

allsets=set(allphones)
setphone18w=set(phone18w)
setphone8w=set(phone8w)
allsetw=set(phone8w) | setphone18w
crosssetw= set(phone8w) & setphone18w
crossset=allsets & allsetw
valueset=allsetw - crossset

sbaby8=set(baby8)
crossset8=setphone18w & sbaby8
vs=valueset | crossset8
cs=allsetw - vs
#cs=set(phone8w) & sphone8
#vs=set(phone8w) -cs

print len(allsetw)
print len(vs)
print len(cs)

true87w=open(args.output_csv,"w")

#true87=csv.writer(true87w)
true87=UnicodeWriter(true87w)
#misaddrw=open(args.mis_csv,"w")
#misaddr=UnicodeWriter(misaddrw)
wrong87w=open(args.dup_csv,"w")
#wrong87=csv.writer(wrong87w)
wrong87=UnicodeWriter(wrong87w)
wrong87.writerow(headers)
for c in cs:
    wrong87.writerow(dict87w[c])
wrong87w.close()

true87.writerow(headers)
for v in vs:
    content=procaddress(dict87w[v])
    true87.writerow(content)

true87w.close()
#misaddrw.close()

