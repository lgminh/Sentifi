__author__ = 'minhloc'
# from common.mongo import MongoRepo, connect
# from common.messaging import MessageProducer

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
import unicodecsv
import datetime as DateTime
import numpy as np









def main():

     # #check valid time
     # invalid_time = True
     #
     # while(invalid_time==True):
     #     year,month,date = raw_input("From date:").split()
     #     # #get begin date
     #     days = []
     #     days.append(DateTime.date(int(year),int(month),int(date)))
     #     #
     #     # #get end date
     #     year,month,date = raw_input("To date:").split()
     #     days.append(DateTime.date(int(year),int(month),int(date)))
     #     #check valid time
     #     if (days[0] > days[1]):
     #         print "invalid time!!!"
     #     else:
     #         invalid_time = False
     #
     #
     # #check valid keyword
     # invalid_keyword = True
     # #get keyword
     # while(invalid_keyword==True):
     #     keyword = str(raw_input("keyword:"))
     #     if not keyword:
     #         print "Null keyword!!!!"
     #     else:
     #         invalid_keyword = False

     #raw_input("Number of ")

     Dict_01 = {'a':2,'b':5,'c':12}
     Dict_02 = {'b':10,'c':7,'t':14,'g':30}
     Dict_03 = {'b':15,'a':34,'t':27}



     myList = []
     myList.append(Dict_01)
     myList.append(Dict_02)
     myList.append(Dict_03)

     mySet = set(Dict_01.keys())

     for myDict in myList:
         mySet = set(set(dict.keys(myDict))|mySet)

     # for d in myList:
     #    for key,val in enumerate(d):
     #        print key, " ",val
     for d in myList:
        print myList[-1] is d

     with open('test.csv','w') as fi:
          f = unicodecsv.writer(fi,encoding="utf-8")
          for sns_id in sorted(mySet):
             my_str = sns_id

             for d in myList:
                 #check if each key is in curent dictionary
                 if sns_id in dict.keys(d):
                 # check if current dictionary is the last
                     if d is not myList[-1]:
                         my_str += ',' + str(d[sns_id])
                     else:
                         my_str += str(d[sns_id])

             f.writerow(my_str)


if __name__ == '__main__':
    main()


