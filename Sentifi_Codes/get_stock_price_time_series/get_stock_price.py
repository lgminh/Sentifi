#!/usr/bin/python

__author__ = 'minhloc'
from os import listdir
from os.path import isfile, join
import json
from datetime import datetime
from distutils.dir_util import copy_tree
import re
from collections import OrderedDict

from psycopg2 import connect
from psycopg2.extras import RealDictCursor, Json
import unicodecsv
from xml.dom.minidom import parse
import xml.dom.minidom
import xml.etree.ElementTree as ET
PG_HOST = 'psql-dev-1.ireland.sentifi.internal'
PG_DB = 'da0_dev'
PG_USER = 'dbw'
PG_PASS = 'sentifi'
PG_PORT = '5432'

# ROOT_FOLDER = '/home/semantic/projects/sentifi/semantic/scripts/public'
#
# today = datetime.now()
# JSON_FOLDER = join(ROOT_FOLDER, 'taxonomy')
# JSON_FOLDER_NEW = join(ROOT_FOLDER, 'taxonomy_%s' % today.strftime('%Y%m%d')) # create new folder

isin_HSBC="GB0005405286"

file_stock_price = 'stock_prices_HSBC.csv'


def get_pg_connection():
    print 'Connecting to PosgreSQL at %s:%s' % (PG_HOST, PG_PORT)
    return connect(database=PG_DB, user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT)



#get payload history
def get_payload_history(pg_conn, isin):

    pg_cur = pg_conn.cursor()
    print "Retrieving data from daily database..."
    pg_cur.execute('''
    SELECT payload
    FROM fb_stock_price_history
    WHERE fb_stock_price_history.isin = '%s' ''' % (isin))

    #close connection
    #pg_conn.close()
    print "Completed!!!"
    return pg_cur.fetchall()

#get payload daily
def get_payload_daily(pg_conn,isin):

    #connect to DB
    #pg_conn = get_pg_connection()

    #retrieve data
    pg_cur = pg_conn.cursor()
    print "Retrieving data from history database..."
    pg_cur.execute('''
    SELECT payload
    FROM fb_stock_price_daily
    WHERE fb_stock_price_daily.isin = '%s' ''' % (isin))

    #close connection
    #pg_conn.close()
    print "Completed!!!"
    return pg_cur.fetchall()

# get stock_price history
def get_time_series_history(isin):

   #connecto to BD
   pg_conn = get_pg_connection()

   #retrieve data


   #get list of payload history
   [xml_files] = get_payload_history(pg_conn, isin)

   #close connection to DB

   #read each payload
   for xml_file in xml_files:

       xml_tree = ET.fromstring(xml_file)

       #write file
       with open(file_stock_price,'w') as f:
           fw = unicodecsv.writer(f,encoding='utf-8')
           fw.writerow(('time','stock_price'))
           for element in xml_tree.iter():
               time = ""
               stock_price = ""
               if element.attrib.get('d'):
                    time = element.attrib.get('d')

               if element.attrib.get('c'):
                    stock_price = element.attrib.get('c')

               if time is not '' and stock_price is not '':
                    fw.writerow((time, stock_price))

   pg_conn.close()

# get stock_price history
def get_time_series_daily(isin):

   #connecto to BD
   pg_conn = get_pg_connection()

   #retrieve data
   #pg_cur = pg_conn.cursor()

   #get list of payload daily

   #get list of payload daily from DB
   xml_files = get_payload_daily(pg_conn, isin_HSBC)

   #read each payload, write file 'stock_price_HSBC'
   with open(file_stock_price,'a') as f:
       fr = unicodecsv.writer(f,encoding='utf-8')
       for xml_file in xml_files:

           #parse xml file
           xml_tree = ET.fromstring(xml_file[0])
           #root = xml_tree.getroot()

           for element in xml_tree.iter():
                #get time
                if element.tag == 'time':
                    time = element.text
                #    print time
                #get stock price
                if element.tag == 'close':
                    stock_price = element.text
                #   print stock_price
           fr.writerow((time, stock_price))

   pg_conn.close()

def get_stock_price(isin):
    #get  history, write csv file
    get_time_series_history(isin)
    #get  daily, append csv file above
    get_time_series_daily(isin)


# load stock_price from csv file
def load_stock_price(file_stock_price):
    with open(file_stock_price,'r') as reader:
        fr = unicodecsv.reader(reader,encoding='utf-8')
        stock_prices = []
        for element in fr:
            #time_serie = datetime.strptime(element[0],"%Y-%M-%D %H:%M:%S")
            #stock_price = float(element[0])
            #stock_prices.append((time_serie,stock_price))
            stock_prices.append((element[0],element[1]))

        for stock_price in stock_prices:
            print stock_price[0], stock_price[1]



        return stock_prices






if __name__ == '__main__':
    #main()
    #get_time_stock_price(0)
    get_stock_price(isin_HSBC)
    load_stock_price(file_stock_price)