#!usr/bin/python

import tensorflow as tf
import  numpy as np
from tensorflow.examples.tutorials.mnist import  input_data

#mnist = input_data.read_data_sets('MNIST_data',one_hot=True)

from os import  listdir
from os.path import isfile, join
import json
from datetime import  datetime
from distutils.dir_util import copy_tree
import re
from collections import OrderedDict
from psycopg2 import connect
from psycopg2.extras import RealDictCursor


PG_HOST = 'psql-dev-1.ireland.sentifi.internal'
PG_DB = 'da0-dev'
PG_USER = 'dbw'
PG_PASS = 'sentifi'
PG_PORT = '5432'


ROOT_FOLDER = '/home/projects/sentifi/'
JSON_FOLDER = join(ROOT_FOLDER,'taxonomy')
JSON_FOLDER_NEW = join(ROOT_FOLDER,'taxonomy_%s' %(datetime.strftime('%Y%m%d')))


def get_pg_connection():
    print "Connecting to PosgreSQL at %s:%s" % (PG_HOST,PG_PORT)
    return connect(database=PG_HOST,user=PG_USER,password=PG_PASS,host=PG_PORT)


def main():
    print "hello"


def modify_pattern_db():
    pg_conn = get_pg_connection()


def modify_rule_lang(rule):
    if 'id' in rule:
        if '_EN' in rule['id']:
            rule['id'] = rule['id'].replace('_EN','')
            rule['lang'] = 'EN'
        elif '_DE' in rule['id']:
            rule['id'] = rule['id'].replace('_DE','')
            rule['lang'] = 'DE'
    return rule

def modify_rule_dictionaries(rule):
    if 'dictionaries' in rule:
        dictionaries = rule['dictionaries']
        for idx, dictionary in enumerate(dictionaries):
            if 'is_not' in dictionary:
                dictionary['isNot']  = dictionary['is_not']
                del dictionary['is_not']
            dictionaries[idx] = dictionary
            rule['dictionaries'] = dictionaries
    return rule


def replace_value():
    print 0

def modify_rule(rule):
    if 'pattern' in rule:
        patterns = rule['pattern']
        for idx, pattern in enumerate(patterns):
           pattern = replace_value(pattern)
           patterns[idx] = pattern
        rule['pattern']  = patterns
    if 'type' in rule:
        del rule['type']
    return rule



if __name__ == '__main__':
    main()
