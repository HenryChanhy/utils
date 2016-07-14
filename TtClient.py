#-*- coding: utf8 -*-
__author__ = 'Henry'

import argparse
import csv
#import requests
from urllib3 import HTTPConnectionPool
from multiprocessing.dummy import Pool as ThreadPool
#import resource
#resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))

parser = argparse.ArgumentParser()
parser.add_argument("input_csv")
parser.add_argument("-e","--input_err_csv")
parser.add_argument("-H","--input_his_csv")
args = parser.parse_args()
namesplit= args.input_csv.split(".")[0]
if args.input_err_csv==None:
    args.input_err_csv=namesplit+".err.csv"
if args.input_his_csv==None:
    args.input_his_csv="tid.txt"

def getpara(out_id,statue,msg=None):
    url="http://int.taotonggroup.com/pampers1/TtApi/test_TOS.json?appKey=updateandtos&"
    if msg!=None:
        return url+"out_tid="+out_id+"&status="+statue+"&msg="+msg
    else:
        return url+"out_tid="+out_id+"&status="+statue
#tid=[]
#for i in open(args.input_his_csv, 'rb'):
#    tid.append(i)
def loadhistorytolist(list):
    with open(args.input_his_csv, 'rb') as f:
        reader = csv.reader(f)
#        headers = reader.next()
        for row in reader:
            list.append(row[0])
    f.close()

def loadfiletolist(list,hislist):
    with open(args.input_csv, 'rb') as f:
        reader = csv.reader(f)
        headers = reader.next()
        for row in reader:
            if row[2] in hislist:
                continue
            else:
                list.append(getpara(row[2],"3"))
    f.close()
    with open(args.input_err_csv,'rb') as f:
        reader = csv.reader(f)
        headers = reader.next()
        for row in reader:
            if row[2] in hislist:
                continue
            else:
                list.append(getpara(row[2],"2",row[16]))
#            list.append(getpara(row[2],"2",row[16].encode('utf8')))
#            q.put(data)
#            result=updateTOS(data)
            #log_file("data:%s result:%s"%(data,result))
    f.close()

def Httprequest_helper(args):
    return Httppool.request("GET",args)

if __name__ == '__main__':
#    q = Queue()
    list=[]
    tid=[]
    loadhistorytolist(tid)
    loadfiletolist(list,tid)
    print len(list)
    print "pushing data"
    # Make the Pool of workers
    pool = ThreadPool(8)
    Httppool = HTTPConnectionPool('int.taotonggroup.com', maxsize=8)

    # Open the urls in their own threads
    # and return the results
    pool.map(Httprequest_helper, list)
    #close the pool and wait for the work to finish
    pool.close()
    pool.join()

