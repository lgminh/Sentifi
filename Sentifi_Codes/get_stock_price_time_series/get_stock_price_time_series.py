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
from psycopg2.extras import RealDictCursor #, Json

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

# today = datetime.now()
# JSON_FOLDER = join(ROOT_FOLDER, 'taxonomy')
# JSON_FOLDER_NEW = join(ROOT_FOLDER, 'taxonomy_%s' % today.strftime('%Y%m%d')) # create new folder

isin_HSBC="GB0005405286"

stock_price_file = 'stock_prices_HSBC.csv'

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
def get_payload_daily(pg_conn, isin):

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
       with open(stock_price_file,'w') as f:
           fw = unicodecsv.writer(f,encoding='utf-8')
           fw.writerow(('time','stock_price'))
           for element in xml_tree.iter():
               #get time_serie, stock_price
               time = ""
               stock_price = ""
               if (element.attrib.get('d') and element.attrib.get('c')):
                   time = element.attrib.get('d') # get time
                   stock_price = element.attrib.get('c') # get stock price
                   fw.writerow((time, stock_price))


   pg_conn.close()

# get stock_price daily
def get_time_series_daily(isin):

   #connecto to BD
   pg_conn = get_pg_connection()

   #retrieve data
   #pg_cur = pg_conn.cursor()

   #get list of payload daily

   #get list of payload daily from DB
   xml_files = get_payload_daily(pg_conn, isin_HSBC)

   #read each payload, write file 'stock_price_HSBC'
   with open(stock_price_file,'a') as f:
       fr = unicodecsv.writer(f,encoding='utf-8')
       for xml_file in xml_files:

           #parse xml file
           xml_tree = ET.fromstring(xml_file[0])
           #root = xml_tree.getroot()

           for element in xml_tree.iter():
                #get time
                if element.tag == 'time':
                    time = element.text
                #get stock price
                if element.tag == 'close':
                    stock_price = element.text

           fr.writerow((time, stock_price))

   pg_conn.close()

def get_stock_price(isin):
    #get  history, write csv file
    get_time_series_history(isin)
    #get  daily, append csv file above
    get_time_series_daily(isin)

#read csv stock file
def read_stock_price():
    with open(stock_price_file,'r') as fr:
        f = unicodecsv.reader(fr,encoding='utf-8')
        stock_prices = []

        for row in f:
            stock_prices.append((row))

        for element in stock_prices:
            print element[0], element[1]

    return stock_prices


if __name__ == '__main__':
    get_stock_price(isin_HSBC)
    read_stock_price()
