#-*- coding: utf8 -*-
__author__ = 'Henry'


import urllib2
from multiprocessing.dummy import Pool as ThreadPool
import resource
import time
#resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))


def getpara(dt):
    # url="https://int.taotonggroup.com/pampers1/TtApi/test_TOS.json?appKey=updateandtos&"
    return "http://app.caijing.com.cn/?app=digg&controller=digg&action=digg&contentid=4106728&jsoncallback=jQuery1102004171008268256027_"+str(time.time())+"&flag=1&_="+str(time.time())+str(dt)

    # if msg!=None:
    #     return url+"out_tid="+out_id+"&status="+statue+"&msg="+msg
    # else:
    #     return url+"out_tid="+out_id+"&status="+statue
#tid=[]
#for i in open(args.input_his_csv, 'rb'):
#    tid.append(i)


if __name__ == '__main__':
    for i in range(1,10000):
        results = urllib2.urlopen(getpara(i))
        print results
    #close the pool and wait for the work to finish


