#-*- coding: utf8 -*-
__author__ = 'Henry'

import argparse
import csv
import pymysql.cursors

parser = argparse.ArgumentParser()
parser.add_argument("input_csv")
parser.add_argument("-e","--input_err_csv")
parser.add_argument("-H","--input_his_csv")
args = parser.parse_args()

def updatemysql():
    connection = pymysql.connect(host='localhost',user='taotonguser',password='test578239',db ='taotongdb',charset='utf8',cursorclass=pymysql.cursors.DictCursor)
    try:
        with open(args.input_csv, 'rb') as f:
            reader = csv.reader(f)
            headers = reader.next()
            for row in reader:
                with connection.cursor() as cursor:
                    # Create a new record
                    #sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
                    #sql = "UPDATE 'trade' SET 'province'=prov, 'city'=city ,'area'=area,'address'=detail WHERE 'out_tid''=tid"
                    sql = "UPDATE trade SET province=%s, city=%s ,area=%s,address=%s WHERE out_tid=%s"
                    cursor.execute(sql, (row[7],row[8],row[9],row[10], row[2]))
                # connection is not autocommit by default. So you must commit to save
                # your changes.
                connection.commit()
    finally:
        connection.close()
        f.close()

if __name__ == '__main__':
#    q = Queue()
    updatemysql()
