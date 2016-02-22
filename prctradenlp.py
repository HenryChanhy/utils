#! /usr/bin/env python2.7
#-*- coding: utf8 -*-
import csv, codecs, cStringIO
from snownlp import SnowNLP
import argparse
import re
#p=re.compile("[\s+\.\!\/\|\,_,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）＠]+".decode("utf8"))
p=re.compile("[\.\!\|\,,$%^*+\"\']+|[+！，。？、~@￥%……&*＠]+".decode("utf8"))
pspace=re.compile("\s+".decode("utf8"))
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
PCA={} #prov,city,area
areas=[]
CAS={} #city,area,street
street=[]
#加载 省-市对应表
with open("citys.csv", 'rb') as f:
    #reader = csv.reader(f)
    reader = UnicodeReader(f)
#    headers = reader.next()
    for row in reader:
        citys.append(row[1])
        dictcity[row[1]]=row[0]
f.close()

#加载省-市-区 对应表
with open("pca.csv",'rb') as f:
    reader = UnicodeReader(f)
    for row in reader:
        areas.append(row[3])
        if not PCA.has_key(row[4]):
            PCA[row[4]]={}
        PCA[row[4]][row[3]]=row[5]
f.close()

#加载市-区-街道 对应表
with open("street.csv",'rb') as f:
    reader=UnicodeReader(f)
    for row in reader:
        if row[1] in zhixia:
            row[2]=row[1]+u"市"
        if row[2] == u"市辖区" or row[2] ==u"县" or row[2].endswith(u"直辖县级行政区划"):
            row[2]=row[3]
        elif row[2]==u"铜仁市":
            row[2]=u"铜仁地区"
        elif row[2]==u"毕节市":
            row[2]=u"毕节地区"
        street.append(row[4])
        if not CAS.has_key(row[2]):
            CAS[row[2]]={}
        CAS[row[2]][row[4]]=row[3]
f.close()

#用城市 查省信息
def findprovincebycity(straddr):
    if straddr in dictcity:
        return getprovince(dictcity[straddr])
    elif straddr+u"市" in dictcity:
        return getprovince(dictcity[straddr+u"市"])
    else:
        return None

#查省信息
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

#查市信息
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

#查区信息
def getarea(straddrfilter):
    straddr=filterotherarea(straddrfilter)
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

#用省,区 查市
def getcitybyProvArea(prov,straddr):
#    if prov in zhixia:
#        prov+=u"市"
    if PCA[prov].has_key(getarea(straddr)):
        return PCA[prov][getarea(straddr)]
#    elif PCA[prov].has_key(straddr+u"区"):
#        return PCA[prov][straddr+u"区"]
#    elif PCA[prov].has_key(straddr+u"市"):
#        return PCA[prov][straddr+u"市"]
    else:
        return None

def findstreetincity(city,straddr):
    if CAS.has_key(city):
        for s in CAS[city].keys():
            if s in straddr:
                return s
    return None

#从字符串开始 去重1次
def filterdupaddress(straddr):
    dup=0
    for dup in range(len(straddr)/2,0,-1):
        if dup > 3:
            if straddr[:dup] in straddr[dup:]:
#            print dup
                break
        else:
            if straddr[:dup] in straddr[dup:dup+len(straddr[:dup])]:
#            print dup
                break

    if dup > 1:
        dupsplit=straddr.rsplit(straddr[:dup],1)
        return dupsplit[0]+dupsplit[1]
#        return straddr[dup:]
    else:
        return straddr

#根据分词去重
def dynareduce(address):
    addwords=SnowNLP(address).words
    for pos in range(0,len(addwords)-1):
        for pos1 in range(pos+1,len(addwords)-1):
            if len(addwords[pos]) >1 and addwords[pos] == addwords[pos1] and not addwords[pos].isdigit():
                addwords[pos1]=u""
                break
    result=u""
    for words in addwords:
        result+=words
    return filterdupaddress(result)

#根据省市区详细地址 重构详细地址.
def setdetail(prov,city,area,detail):
    baredetail=detail.replace(area,u"").replace(city,u"").replace(prov,u"")
    if prov in zhixia:
        return city+area+baredetail
    elif city==area:
        return prov+city+baredetail
    else:
        return prov+city+area+baredetail

#纠正区域信息
def filterotherarea(straddr):
    if u"其它区" in straddr:
#        addrsplit = straddr.split(u"其它区",1)
        return straddr.replace(u"其它区",u"")
    elif u"市辖区" in straddr:
        return straddr.replace(u"市辖区",u"")
    elif u"墉桥区" in straddr:
        return straddr.replace(u"墉桥区",u"埇桥区")
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
        return straddr.replace(u"铜仁市",u"铜仁地区")
    elif u"东陵区" in straddr:
        return straddr.replace(u"东陵区",u"浑南区")
    elif u"浑南新区" in straddr:
        return straddr.replace(u"浑南新区",u"浑南区")
    else:
        return straddr

#是否模糊字结尾
def isfuzzyend(straddr):
    for i in fuzzywords:
        if straddr.endswith(i):
            return True
    return False

#处理 地址字段 要求 contnet[3] 省 content[4] 市 conten[5] 区 content[6] 详细地址
def procaddress(content):
    addr=None
    city=None
    area=None
    fulladdr=u""
    if len(content)>7:
        content.append(content[6])
        Detailaddress=pspace.sub("",content[6])
        content.append(u"")
        content.append(u"")
        if content[3]==u"-":
            content[3]=u""
            content[4]=u""
            content[5]=u""
            Detailaddress=paddress.sub("",content[6])
        else:
            if Detailaddress.startswith(content[3]):
                Detailaddress=Detailaddress.replace(content[3],"",1)
            if Detailaddress.startswith(content[4]):
                Detailaddress=Detailaddress.replace(content[4],"",1)
            if Detailaddress.startswith(content[5]):
                Detailaddress=Detailaddress.replace(content[5],"",1)

        content[6]=Detailaddress
    else:
#        print content
        content.append(u"数据不完整")
        return content
    address_status=True
#   详细地址字段组合
#    for contents in content[7:]:
#        content[6]+=contents
    if content[3]!='':
        addr=getprovince(content[3])
    if content[4]!='':
        city=getcity(content[4])
    if content[5]!='': #有详细地址字段
        area=getarea(content[5])

    if addr!=None and city!=None and area!=None:
        content[3]=addr
        content[4]=city
        content[5]=area
        if content[6].startswith(addr) and city in content[6] and area in content[6]:
            fulladdr=content[6]
        elif area in content[6] and city in content[6]:
            fulladdr=addr+content[6]
        elif area in content[6]:
            fulladdr=addr+area+content[6]
        else:
            if addr in zhixia:
                fulladdr=city+area+content[6]
            else:
                fulladdr=addr+city+area+content[6]
        content[6]=fulladdr
        area=getarea(area)
        if city == u"":
            if addr in zhixia:
                city=addr+u"市"
            else:
                city = area
            content[4]=city
        else:
            content[4]=city

#         s=SnowNLP(content[6])
#         addrs=s.words
#         if len(addrs) > 1:
#             if addrs[0] in provincesf:
#                 i=0
#             elif addrs[0] in zhixia :
#                 content[6]=addrs[0]+u"市"
#                 for c in addrs[1:]:
#                     if not content[6].endswith(c):
#                         content[6]+=c
#
#             elif addrs[0] in zizhi:
#                 content[6]=addrs[0]+u"自治区"
#                 pos=1
#                 if addrs[pos] == u"省" or addrs[pos].endswith(u'直辖县级行政区划') or addrs[pos].endswith(u'自治区'):
#                     pos+=1
#                 for c in addrs[pos:]:
#                     if not content[6].endswith(c):
#                         content[6]+=c
#             elif addrs[0] == u"新疆":
#                 content[6]=u"新疆维吾尔自治区"
#                 pos=1
#                 if addrs[pos] == u"省" or addrs[pos].endswith(u'直辖县级行政区划') or addrs[pos] ==u"维吾尔":
#                     pos+=1
#                 if addrs[pos].endswith(u'自治区'):
#                     pos+=1
#                 for c in addrs[pos:]:
#                     if not content[6].endswith(c):
#                         content[6]+=c
#             elif addrs[0] == u"广西":
#                 content[6]=u"广西壮族自治区"
#                 pos=1
#                 if addrs[pos] == u"省" or addrs[pos].endswith(u'直辖县级行政区划') or addrs[pos] ==u"壮族":
#                     pos+=1
#                 if addrs[pos].endswith(u'自治区'):
#                     pos+=1
#                 for c in addrs[pos:]:
#                     if not content[6].endswith(c):
#                         content[6]+=c
#             elif addrs[0] == u"宁夏":
#                 content[6]=u"宁夏回族自治区"
#                 pos=1
#                 if addrs[pos] == u"省" or addrs[pos].endswith(u'直辖县级行政区划') or addrs[pos] ==u"回族":
#                     pos+=1
#                 if addrs[pos].endswith(u'自治区'):
#                     pos+=1
#                 for c in addrs[pos:]:
#                     if not content[6].endswith(c):
#                         content[6]+=c
#             elif addrs[0] in provinces:
#                 content[6]=addrs[0]+u"省"
#                 pos=1
#                 if addrs[pos] == u"省" or addrs[pos].endswith(u'直辖县级行政区划'):
#                     pos+=1
#                 for c in addrs[pos:]:
#                     if not content[6].endswith(c):
#                         content[6]+=c
#             elif addrs[0] in zhixiashi:
#                 content[6]=addrs[0]
#                 for c in addrs[1:]:
#                     if not content[6].endswith(c):
#                         content[6]+=c
#             else:
#                 if content[3] in zhixia:
#                     addrfull=u""
#                 else:
#                     addrfull=content[3]
# #                    if content[5] == u"其它区" or content[5]==u"市辖区":
#                 content[5] = area
#                 for contents in content[4:7]:
#                     addrfull+=contents
#                 content[6]=addrfull
# #            content[5] == area:
#             content[6] = dynareduce(filterotherarea(content[6]))
#         else:#內容過短
# #            address_status=False
#             content[13]+=u"详细地址太短"
    else:#没有省市区字段，需拆分详细地址
        if addr!=None:
            fulladdr+=addr
        if city!=None:
            fulladdr+=city
        if area!=None:
            fulladdr+=area
        fulladdr+=content[6]
#        fulladdr=dynareduce(content[3]+content[4]+content[5]+content[6])
        content[6]=fulladdr
        posp=0
        posc=0
        posa=0
#        content[13]+=u"省市区信息不对,"
#        for contents in content[3:]:
#        content[6]=fulladdr
        addrs=SnowNLP(fulladdr).words
        if addr==None:
            addr=getprovince(content[3])
        if addr==None:
            addr=findprovincebycity(content[4])
        if addr==None:
            for posp in range(0,len(addrs)-1):
                addr=getprovince(addrs[posp])
                if addr!=None:
                    break
        if addr==None:
            for posp in range(0,len(addrs)-1):
                addr=findprovincebycity(addrs[posp])
                if addr!=None:
                    break
        if addr==None:
            addr=u""
            content[13]+=u"找不到省信息,"
        else :
            if addr in zhixia:
                content[6]=u""
            else:
                content[6]=addr
            if city==None and content[4]!=u"":
                city=getcity(content[4])
            if city==None and content[5]!=u"":
                city=getcity(content[5])
            if city==None and content[4]!=u"":
                city=getcitybyProvArea(addr,content[4])
                if city!=None:
                    area=getarea(content[4])
            if city==None and content[5]!=u"":
                city=getcitybyProvArea(addr,content[5])
                if city!=None:
                    area=getarea(content[5])
            if city==None:
                for posc in range(posp,len(addrs)-1):
                    city=getcity(addrs[posc])
                    if city!=None:
                        break
            if city==None or findprovincebycity(city)!=addr:
                city=u""
                content[13]+=u"找不到市信息,"
            else:
                content[6]+=city
                if area ==None and content[5]!=u"":
                    area=getarea(content[5])
                if area==None:
                    for posa in range(posc,len(addrs)-1):
                        area=getarea(addrs[posa])
                        if area!=None:
                            break
                if area==None or getcitybyProvArea(addr,area)!= city:
                    area=u""
                    posa=1
                    content[13]+=u"找不到区信息"
                else:
                    content[6]+=area
                    if city==u"":
                        city=area
                if posa > 0:
                    posa+=1
                for detail in addrs[posa:]:
                    content[6]+=detail
        content[3]=addr
        content[4]=city
        content[5]=area
        content[6]=dynareduce(filterotherarea(content[6]))

    for pos in range(0,len(content)-1):
        if content[pos]==None:
            content[pos]=u""
    content[6]=setdetail(content[3],content[4],content[5],content[6])

    if isfuzzyend(content[6]):
        content[13]+=u"模糊字结尾"
#        address_status = False
    street=findstreetincity(content[4],content[6])
    if street!=None:
        content[14]=street
        content[6]=content[6].replace(street,u"")

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
    headers.append("street")
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
#baby8=[]
#for i in open("baby8uniq.txt"):
#    baby8.append(i[0:11])

#sphone8=set(phone8)
#sphone18=set(phone18)
#allsets=sphone8 | sphone18

allsets=set(allphones)
setphone18w=set(phone18w)
setphone8w=set(phone8w)
allsetw=set(phone8w) | setphone18w
cs=allsetw & allsets
vs=allsetw - cs

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
#    print v
    content=procaddress(dict87w[v])
    true87.writerow(content)

true87w.close()
#misaddrw.close()

