#! /usr/bin/env python2.7
#-*- coding: utf8 -*-
import csv, codecs, cStringIO
from snownlp import SnowNLP
import argparse
import re
from hanziconv import HanziConv
from datetime import datetime
#p=re.compile("[\s+\.\!\/\|\,_,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）＠]+".decode("utf8"))
p=re.compile("[\.\!\|\,\\\$%^*+=\"\']+|[+！，。？、~@￥%……&*＠]+".decode("utf8"))
pspace=re.compile("\s+".decode("utf8"))
ptmspace=re.compile("\s*\(TM2\)".decode("utf8"))
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
fid = open('ntctradeid.txt', 'r')
trade_id=fid.read()
fid.close()
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
shop_id={u"蜜芽宝贝":"98",u"BV-育儿网":"93",u"BV-妈妈帮":"94",u"BV-辣妈帮":"95",u"微官":u"29",
         u"积分兑换玩具&正装":"49",u"积分兑换试用装":"48",u"BV-PCbaby":"23",u"BBS-ISA":u"64",
         u"BOX":"38",u"特卖":"36",u"贝贝":u"106",u"广点通":"63",u"干货挑战":"103"}
gdt_barcode={u"8片S码":u"6903148235508",u"8片NB码":u"6903148235515"}
pd_barcode={u"4片L码": "6903148215159", u"8片S码": "6903148238912", u"8片NB码": "6903148238905",
              u"12片NB码": "6903148240014", u"12片S码": "6903148240021"}
pg_barcode={u"帮宝适敏感肌肤系列婴儿湿巾56片（买赠）":u"201503310048",u"帮宝适特级棉柔系列中包装初生型NB(32+10片）":u"6903148126233",u"帮宝适特级棉柔系列中包装小号S(28+5片）":u"2015040910060",u"帮宝适特级棉柔系列中包装中号M(30片）":u"2015040910061",u"帮宝适特级棉柔系列中包装大号L(25片）":u"2015040910062",u"帮宝适特级棉柔系列中包装加大号XL(20片）":u"2015040910063",u"炫动交通组":u"33320154100001",u"交通工具积木船":u"33320154100003",u"费雪二合一狮子踏行车":u"33320154100004",u"叠叠互动小火车":u"33320154100005",u"费雪缤纷动物拼图":u"33320154100006",u"音乐爬行摇摇球":u"33320154100007",u"音乐小骑(摇)马":u"33320154100008",u"皇室音乐嘘嘘乐":u"33320154100009",u"奇趣学步车":u"33320154100010",u"蹦跳欢乐园":u"33320154100011",u"超薄干爽系列中包装加大号23片(L)":u"6903148158333",u"帮宝适特级棉柔系列首发装中号56+2片老包装M":u"6903148204849",u"帮宝适特级棉柔系列首发装加大号36+2片老包装XL":u"6903148095034",u"小泰克消防站大冒险":u"33320154100050",u"澳贝响铃滚滚球":u"33320154100051",u"蓝盒宝宝捶捶乐":u"33320154100052",u"蓝盒宝宝宠物酒店":u"33320154100053",u"蓝盒宝宝学习桌椅套装":u"33320154100054",u"蓝盒宝宝龟摇铃":u"33320154100055",u"帮宝适拉拉裤中包装中号M24片(男)":u"6903148081815",u"帮宝适拉拉裤中包装中号M24片(女)":u"6903148081822",u"帮宝适拉拉裤中包装大号L21片(女)":u"6903148081846",u"澳贝电子吉他":u"33320154100072",u"澳贝音乐梦工厂":u"33320154100073",u"炫动小摇铃":u"33320154100074",u"精灵金猪":u"33320154100075",u"蓝盒宝宝可爱蜘蛛推车":u"33320154100076",u"帮宝适特级棉柔系列中包装初生型NB(32+10片）":u"33320154100082",u"帮宝适特级棉柔系列中包装小号S(28+5片）":u"33320154100083",u"帮宝适特级棉柔系列中包装大号L(25片）":u"6903148094983",u"澳贝青蛙小鼓":u"33320154100090",u"澳贝乐队床铃":u"33320154100091",u"澳贝音乐踏行车":u"33320154100092",u"帮宝适特级棉柔系列初生型NB(72片)":u"6903148167915",u"帮宝适特级棉柔系列中包装小号33片":u"6903148126240",u"帮宝适拉拉裤男中号":u"2片装",u"帮宝适超薄干爽系列小号4片装小号S":u"6903148214947",u"帮宝适特级棉柔系列中号4片装M（赠品）":u"6903148204580",u"帮宝适超薄干爽系列小号4片装小号S":u"6903148214947",u"帮宝适特级棉柔系列大号4片装L（赠品）":u"6903148204597",u"帮宝适特级棉柔系列小号4片装S（赠品）":u"6903148213803",u"帮宝适拉拉裤中号（男）M2片装（赠品）":u"6903148212479",u"帮宝适拉拉裤中号（女）M2片装（赠品）":u"6903148212486",u"帮宝适拉拉裤大号（男）L2片装（赠品）":u"6903148212493",u"帮宝适拉拉裤大号（女）L2片装（赠品）":u"6903148212509",u"帮宝适超薄干爽婴儿纸尿裤4片装小号（S）":u"6903148196106",u"帮宝适超薄干爽婴儿纸尿裤4片装加大号（XL）":u"6903148215166",u"帮宝适特级棉柔拉拉裤中号2片装（男女通用）M":u"6903148229637"}
pg_trial=(u"帮宝适拉拉裤男中号",u"帮宝适超薄干爽系列小号4片装小号S",u"帮宝适特级棉柔系列中号4片装M（赠品）",u"帮宝适超薄干爽系列小号4片装小号S",u"帮宝适特级棉柔系列大号4片装L（赠品）",u"帮宝适特级棉柔系列小号4片装S（赠品）",u"帮宝适拉拉裤中号（男）M2片装（赠品）",u"帮宝适拉拉裤中号（女）M2片装（赠品）",u"帮宝适拉拉裤大号（男）L2片装（赠品）",u"帮宝适拉拉裤大号（女）L2片装（赠品）",u"帮宝适超薄干爽婴儿纸尿裤4片装小号（S）",u"帮宝适超薄干爽婴儿纸尿裤4片装加大号（XL）",u"帮宝适特级棉柔拉拉裤中号2片装（男女通用）M")
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
fixaddress = {u"其它区": u"", u"市辖区": u"", u"地址": u"", u"墉桥区": u"埇桥区", u"璧山区": u"璧山县", u"高陵区": u"高陵县", u"富阳区": u"富阳市",
              u"藁城区": u"藁城市", u"溧水区": u"溧水县", u"南溪区": u"南溪县", u"鹿泉区": u"鹿泉市", u"大足区": u"大足县",
              u"毕节市": u"七星关区", u"铜仁市": u"铜仁地区", u"浑南新区": u"浑南区", u"东陵区": u"浑南区", u"赣榆区": u"赣榆县",
              u"铜梁区": u"铜梁县", u"上虞区": u"上虞市", u"广州省": u"广东省", u"广西省": u"", u"临河市": u"临河区",
              u"娄底地区": u"娄底市", u"海东市": u"海东地区", u"菏泽地区": u"菏泽市", u"巴彦淖尔盟": u"巴彦淖尔市", u"+": u"＋",
              u"望城区": u"望城县", u"增城区": u"增城市", u"溧水区": u"溧水县", u"江都区": u"江都市", u"清新区": u"清新县",
              u"文山市": u"文山县", u"从化区": u"从化市", u"潮安区": u"潮安县", u"电白区": u"电白县", u"藁城区": u"藁城市", u"陵城区": u"陵县",
              u"南康区": u"南康市", u"綦江区": u"綦江县", u"增城区": u"增城市", u"吴江区": u"吴江市", u"铜山区": u"铜山县", u"文登区": u"文登市",
              u"杨凌区": u"杨陵区", u"长安县": u"长安区", u"颖州区": u"颍州区", u"高淳区": u"高淳县", u"姜堰区": u"姜堰市",
              u"胶南市": u"黄岛区", u"徐水区": u"徐水县",
              # u"县县": u"县", u"市市": u"市", u"镇镇": u"镇", u"区区": u"区",
              u"娄底地区": u"娄底市", u"直辖县级行政区划": u"", u"市辖区": u""}
citys=[]
dictcity={}
PCA={} #prov,city,area
areas=[]
CAS={} #city,area,street
street=[]
cityclass={}
areaclass={}
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

#加载城市分级表
with open("cityclass.csv",'rb') as f:
    reader=UnicodeReader(f)
    for row in reader:
        if not cityclass.has_key(row[3]):
            cityclass[row[3]]=row[7]
        if row[5] not in row[3]:
            areaclass[row[5]]=row[7]
f.close()


#查找表头
dictcol={}
dictcol[u"tid"]=(u"ID",u"Reward ID")
dictcol[u"out_tid"]=(u"out_tid",u"订单号")
dictcol[u"mobilPhone"]=(u"手机号",u"mobilPhone",u"mobilePhone",u"手机",u"手机号码",u"申领手机号码",u"电话")
dictcol[u"consignee"]=(u"consignee",u"姓名",u"收件人姓名",u"申领用户名","DELIVERYADDRESS")
dictcol[u"province"]=(u"省",u"province",u"省份")
dictcol[u"city"]=(u"市",u"city")
dictcol[u"area"]=(u"区",u"area",u"区/县")
dictcol[u"address"]=(u"收货地址",u"地址",u"address",u"用户申领地址","addre")
dictcol[u"barCode"]=(u"barCode",)
dictcol[u"address_backup"]=(u"address_backup",u"备份地址")
dictcol[u"remark"]=(u"remark",u"备注",u"说明")
dictcol[u"street"]=(u"street",u"街道")
dictcol[u"in_memo"]=(u"宝宝出生时间",u"预产期/宝宝生日",u"BB日期")
dictcol[u"shop_id"]=(u"渠道",u"渠道")
dictcol[u"order_date"]=(u"注册时间",u"order_date",u"Issue Date")
dictcol[u"product_title"]=(u"商品名称",u"产品名称",u"申领片数",u"Reward Name")
dictcol[u"city_class"]=(u"城市级别",u"城级")
dictcol[u"buyer_id"]=(u"id",u"会员姓名",u"Member Account ID")
dictcol[u"orderGoods_Num"]=(u"orderGoods_Num",)
dictcol[u"plat_type"]=(u"plat_type",)
dictcol[u"pay_status"]=(u"pay_status",)
dictcol[u"storage_id"]=(u"storage_id",)
dictcol[u"order_type"]=(u"order_type",)
dictcol[u"process_status"]=(u"process_status",)
dictcol[u"pay_status"]=(u"pay_status",)
dictcol[u"deliver_status"]=(u"deliver_status",)
dictcol[u"storage_id"]=(u"storage_id",)
dictcol[u"standard"]=(u"standard",)
dictcol[u"wrong_reason"]=(u"DLVRYNBR-REJRSN",)
headercol={}
def checkheader(header):
    emptyfield=[]
    for i in range(0,len(header)-1):
        if len(header[i])==0:
            emptyfield.append(i)
    for field in dictcol:
        for i in range(0,len(header)-1):
            if header[i] in dictcol[field]:
                headercol[field]=i
                break
        if not headercol.has_key(field) and len(emptyfield)>0:
            headercol[field]=emptyfield.pop()
            header[headercol[field]]=field
    return header


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
    if PCA.has_key(prov) and PCA[prov].has_key(getarea(straddr)):
        return PCA[prov][getarea(straddr)]
#    elif PCA[prov].has_key(straddr+u"区"):
#        return PCA[prov][straddr+u"区"]
#    elif PCA[prov].has_key(straddr+u"市"):
#        return PCA[prov][straddr+u"市"]
    else:
        return None

def getcityclass(city,area):
    if areaclass.has_key(area):
        return areaclass[area]
    elif cityclass.has_key(city):
        return cityclass[city]
    else:
        return u""

def findstreetincity(city,area,straddr):
    if CAS.has_key(city):
        for s in CAS[city].keys():
            if s in straddr:
                if CAS[city][s]==area:
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
    if len(address)>0:
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
    else:
        return address

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
    for fixword in fixaddress.keys():
        if fixword in straddr:
            return straddr.replace(fixword,fixaddress[fixword])
    return straddr

#是否模糊字结尾
def isfuzzyend(straddr):
    for i in fuzzywords:
        if straddr.endswith(i):
            return True
    return False

def formatdatetime(strdate):
    try:
        d=datetime.strptime(strdate,"%Y%m%d %H:%M:%S")
    except:
        try:
            d=datetime.strptime(strdate,"%Y-%m-%d")
        except:
            try:
                d=datetime.strptime(strdate,"%m/%d/%Y %H:%M:%S")
            except:
                try:
                    d = datetime.strptime(strdate, "%Y-%m-%d %H:%M")
                except:
                    try:
                        d=datetime.strptime(strdate,"%m/%d/%y %H:%M")
                    except:
                        d=datetime.now()
    return d.strftime("%Y/%m/%d %H:%M:%S")

def checkdate(strdate):
    applydate = datetime.strptime(formatdatetime(strdate), "%Y/%m/%d %H:%M:%S")
    currdate = datetime.now()
    diffday = (currdate-applydate).days
    if diffday > 6:
        result = currdate.strftime("%Y/%m/%d %H:%M:%S")
    else:
        result = strdate
    return result

def formattrade(content, trade_id=None):
    product=HanziConv.toSimplified(content[headercol[u"product_title"]])
    trade=[]
    trade.append(content[headercol[u"tid"]])     
    trade.append(content[headercol[u"buyer_id"]])
    #trade.append(content[headercol[u"out_tid"]])
    trade.append("TT"+str(trade_id))
    trade.append(content[headercol[u"consignee"]])
    #trade.append(formatdatetime(content[headercol[u"order_date"]]))
    orderdate=checkdate(formatdatetime(content[headercol[u"order_date"]]))
    trade.append(orderdate)
    trade.append(content[headercol[u"mobilPhone"]])
    # trade.append(content[headercol[u"shop_id"]])
     #trade.append(content[headercol[u"barCode"]])
    # if shop_id[content[headercol[u"shop_id"]]] == "63":
    #     trade.append(gdt_barcode[content[headercol[u"product_title"]]])
    # else:
    #     trade.append(pd_barcode[content[headercol[u"product_title"]]])
    if pg_barcode.has_key(product):
        trade.append(pg_barcode[product])
    elif product in pg_trial:
        trade.append(pg_trial[product])
    else:
        trade.append(pd_barcode[product])
    trade.append(content[headercol[u"province"]])
    trade.append(content[headercol[u"city"]])
    trade.append(content[headercol[u"area"]])
    trade.append(content[headercol[u"address"]])

    trade.append(product)
    #trade.append(content[headercol[u"orderGoods_Num"]])
    if headercol.has_key(u"shop_id") and shop_id[content[headercol[u"shop_id"]]]=="63":
        trade.append("2")
    else:
        trade.append("1")

    if pg_barcode.has_key(product):
        trade.append(u"49")
    elif product in pg_trial:
        trade.append(u"48")
    else:
        trade.append(shop_id[content[headercol[u"shop_id"]]])
    #trade.append(content[headercol[u"storage_id"]])
    trade.append("38")

    # trade.append(content[headercol[u"express"]])

    # for 邮政选择
    #if content[len(content)-1] == "D":
    #    trade.append(u"邮政国内小包")
    #else:
    #    trade.append(u"惠州申通")
    #trade.append(u"惠州申通") mark on 20160520 by ahao
    trade.append(u"韵达")
    #trade.append(content[headercol[u"pay_status"]])
    trade.append(u"已付款")
    #trade.append(content[headercol[u"is_invoiceOpened"]])
    trade.append(u"0")
    #trade.append(content[headercol[u"order_type"]])
    trade.append(u"正常订单")
    #trade.append(content[headercol[u"order_totalMoney"]])
    trade.append(u"0")
    #trade.append(content[headercol[u"product_totalMoney"]])
    trade.append(u"0")
    #trade.append(content[headercol[u"plat_type"]])
    trade.append("TrialCenter")
    #trade.append(content[headercol[u"cost_Price"]])
    trade.append(u"0")
    trade.append(content[len(content)-1]) #city_class
    #trade.append(content[headercol[u"standard"]])
    trade.append("1")
    trade.append(content[headercol[u"wrong_reason"]])
    if headercol.has_key(u"shop_id"):
        trade.append(content[headercol[u"shop_id"]])
    else:
        trade.append("")

    return trade

#处理 地址字段 要求 contnet[3] 省 content[headercol[u"city"]] 市 conten[5] 区 content[headercol[u"address"]] 详细地址
def procaddress(content):
    addr=None
    city=None
    area=None
    fulladdr=u""
    if len(content)>7:
        Detailaddress=pspace.sub("",content[headercol[u"address"]])
        content.append(Detailaddress)
        content.append(u"")
        content.append(u"")
        if content[headercol[u"province"]]==u"-":
            content[headercol[u"province"]]=u""
            content[headercol[u"city"]]=u""
            content[headercol[u"area"]]=u""
            Detailaddress=paddress.sub("",content[headercol[u"address"]])
        else:
            if Detailaddress.startswith(content[headercol[u"province"]]):
                Detailaddress=Detailaddress.replace(content[headercol[u"province"]],"",1)
            if Detailaddress.startswith(content[headercol[u"city"]]):
                Detailaddress=Detailaddress.replace(content[headercol[u"city"]],"",1)
            if Detailaddress.startswith(content[headercol[u"area"]]):
                Detailaddress=Detailaddress.replace(content[headercol[u"area"]],"",1)

        content[headercol[u"address"]]=Detailaddress
    else:
#        print content
        content.append(u"数据不完整")
        return content
    address_status=True
#   详细地址字段组合
#    for contents in content[7:]:
#        content[headercol[u"address"]]+=contents
    if content[headercol[u"province"]]!='':
        addr=getprovince(content[headercol[u"province"]])
        if addr!=None:
            content[headercol[u"province"]]=addr
        else:
            content[headercol[u"province"]]=u""
    if content[headercol[u"city"]]!='':
        city=getcity(content[headercol[u"city"]])
        if city!=None:
            content[headercol[u"city"]]=city
    if content[headercol[u"area"]]!='': #有详细地址字段
        area=getarea(content[headercol[u"area"]])
        if area!=None:
            content[headercol[u"area"]]=area

    if addr!=None and city!=None and area!=None:
        content[headercol[u"province"]]=addr
        content[headercol[u"city"]]=city
        content[headercol[u"area"]]=area
        if content[headercol[u"address"]].startswith(addr) and city in content[headercol[u"address"]] and area in content[headercol[u"address"]]:
            fulladdr=content[headercol[u"address"]]
        elif area in content[headercol[u"address"]] and city in content[headercol[u"address"]]:
            fulladdr=addr+content[headercol[u"address"]]
        elif area in content[headercol[u"address"]]:
            fulladdr=addr+area+content[headercol[u"address"]]
        else:
            if addr in zhixia:
                fulladdr=city+area+content[headercol[u"address"]]
            else:
                fulladdr=addr+city+area+content[headercol[u"address"]]
        content[headercol[u"address"]]=dynareduce(filterotherarea(fulladdr))
        area=getarea(area)
        if city == u"":
            if addr in zhixia:
                city=addr+u"市"
            else:
                city = area
            content[headercol[u"city"]]=city
        else:
            content[headercol[u"city"]]=city

    else:#没有省市区字段，需拆分详细地址
        if addr!=None:
            fulladdr+=addr
        if city!=None:
            fulladdr+=city
        if area!=None:
            fulladdr+=area
        fulladdr+=content[headercol[u"address"]]
#        fulladdr=dynareduce(content[headercol[u"province"]]+content[headercol[u"city"]]+content[headercol[u"area"]]+content[headercol[u"address"]])
        content[headercol[u"address"]]=fulladdr
        posp=0
        posc=0
        posa=0
#        content[headercol[u"remark"]]+=u"省市区信息不对,"
#        for contents in content[3:]:
#        content[headercol[u"address"]]=fulladdr
        if len(fulladdr)>0 :
            addrs=SnowNLP(fulladdr).words
        else:
            content[headercol[u"remark"]]+=u"数据问题,不能识别到地址."
            print content
            return content
        if addr==None:
            addr=getprovince(content[headercol[u"province"]])
        if addr==None:
            addr=findprovincebycity(content[headercol[u"city"]])
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
            content[headercol[u"remark"]]+=u"找不到省信息,"
        else :
            if addr in zhixia:
                content[headercol[u"address"]]=u""
            else:
                content[headercol[u"address"]]=addr
            if city==None and content[headercol[u"city"]]!=u"":
                city=getcity(content[headercol[u"city"]])
            if city==None and content[headercol[u"area"]]!=u"":
                city=getcity(content[headercol[u"area"]])
            if city==None and content[headercol[u"city"]]!=u"":
                city=getcitybyProvArea(addr,content[headercol[u"city"]])
                if city!=None:
                    area=getarea(content[headercol[u"city"]])
            if city==None and content[headercol[u"area"]]!=u"":
                city=getcitybyProvArea(addr,content[headercol[u"area"]])
                if city!=None:
                    area=getarea(content[headercol[u"area"]])
            if city==None:
                for posc in range(posp,len(addrs)-1):
                    city=getcity(addrs[posc])
                    if city!=None:
                        break
                    city=getcitybyProvArea(addr,addrs[posc])
                    if city!=None:
                        area=getarea(addrs[posc])
                        break
            if city==None or findprovincebycity(city)!=addr:
                city=u""
                content[headercol[u"remark"]]+=u"找不到市信息,"
            else:
                content[headercol[u"address"]]+=city
            if area ==None and content[headercol[u"area"]]!=u"":
                area=getarea(content[headercol[u"area"]])
            if area==None:
                for posa in range(posc,len(addrs)-1):
                    area=getarea(addrs[posa])
                    if area!=None:
                        break
            if area==None or getcitybyProvArea(addr,area)!= city:
                area=u""
                posa=1
                content[headercol[u"remark"]]+=u"找不到区信息"
            else:
                content[headercol[u"address"]]+=area
                if city==u"":
                    city=area
            # if posa > 0:
            #     posa+=1
            for detail in addrs[posa:]:
                content[headercol[u"address"]]+=detail
        content[headercol[u"province"]]=addr
        content[headercol[u"city"]]=city
        content[headercol[u"area"]]=area
        content[headercol[u"address"]]=dynareduce(filterotherarea(content[headercol[u"address"]]))

    for pos in range(0,len(content)-1):
        if content[pos]==None:
            content[pos]=u""
    content[headercol[u"address"]]=setdetail(content[headercol[u"province"]],content[headercol[u"city"]],content[headercol[u"area"]],content[headercol[u"address"]])

    if isfuzzyend(content[headercol[u"address"]]):
        content[headercol[u"remark"]]+=u"模糊字结尾"
#        address_status = False
    street=findstreetincity(content[headercol[u"city"]],content[headercol[u"area"]],content[headercol[u"address"]])
    if street!=None:
        content[headercol[u"street"]]=street
#        content[headercol[u"address"]]=content[headercol[u"address"]].replace(street,u"")
    content[headercol[u"product_title"]]=ptmspace.sub("",content[headercol[u"product_title"]])
    content.append(getcityclass(content[headercol[u"city"]],content[headercol[u"area"]]))
    return content


#for i in range(1,len(allLines)):
#    phone8w.append(allLines[i].split(",")[0])
#    dict87w[allLines[i].split(",")[0]]=i

true87w=open(args.output_csv,"w")
true87=UnicodeWriter(true87w)

# true87=csv.writer(true87w)


# print v
with open(args.input_csv, 'rb') as f:
    #reader = csv.reader(f)
    reader = UnicodeReader(f)
    headers = reader.next()
    header = ["out__tid", "buyer_id", "out_tid", "consignee", "order_date", "mobilPhone", "barCode", "province", "city",
              "area", "address", "product_title", "orderGoods_Num", "shop_id", "storage_id", "express", "pay_status",
              "is_invoiceOpened", "order_type", "order_totalMoney", "product_totalMoney", "plat_type", "cost_Price",
              "city_class", "standard", "wrong_reason","distributor_no"]
    true87.writerow(header)
    headers.append("address_backup")
    headers.append("remark")
    headers.append("street")
    headers.append("cityclass")
    headers=checkheader(headers)

    for row in reader:
        if len(row)<12:
            print row
        else:
            trade_id=int(trade_id)+1
            try:
                content=formattrade(procaddress(row),trade_id)
                true87.writerow(content)
            except Exception as e:
                fid = open('ntctradeid.txt', 'w')
                fid.write(str(trade_id))
                fid.close()
                print e
                raise


true87w.close()
fid = open('ntctradeid.txt', 'w')
fid.write(str(trade_id))
fid.close()
#misaddrw.close()

