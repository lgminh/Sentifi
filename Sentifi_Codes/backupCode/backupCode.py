__author__ = 'minhloc'

from elasticsearch import Elasticsearch
from elasticsearch import helpers
#import pandas as pd
import datetime as DateTime
import unicodecsv

ES_HOST = [
    {'host': 'es0.ssh.sentifi.com', 'port': 9200, 'timeout': 120 },
]


client = Elasticsearch(ES_HOST)


# count total number of message from begin date to end date
def count_msg_from_to(days):
    def exc_query():
        query_count = {
             "query": {
            "range": {
              "published_at": {
                "from": str(days[0]),
                  "to": str(days[1])
              }
            }
          },
          "aggs": {
            "by_sns_id": {
              "terms": {
                "field": "published_by.sns_id",
                "size": 0
              }
            }
          }
        }
        res = client.search(index='rm_searching_staging',doc_type='m',body=query_count)
        return res['aggregations']['by_sns_id']['buckets']
    print "counting message ES... from ", days[0], " to ", days[1]


    #write csv file
    file_name = "count_msg_from_" + str(days[0]) + "_to_" + str(days[1]) + ".csv"
    with open(file_name,'w') as fr:
        f = unicodecsv.writer(fr,encoding='UTF-8')

        #get total count per sns_id
        count_result = {}
        for hit in exc_query():
            count_result[hit['key']] = hit['doc_count']

        #write headline
        f.writerow(('sns_id','num_msg'))

        #write data
        for sns_id, total_count in enumerate(count_result):
            f.writerow((sns_id,total_count))


    print "All done! Please check the result csv file!"

    # return count_result
    return count_result

# count number of message containing keyword
def count_msg_keyword(keyword,days):
    def exc_query():
        query_count_keyword = {
             "query": {
                "bool": {
                  "must": [
                    {
                        "query_string": {
                          "default_field": "ne_classes",
                          "query": keyword
                        }
                    },
                    {
                      "range": {
                        "published_at": {
                          "from": str(days[0]),
                          "to": str(days[1])
                        }
                      }
                    }
                  ]
                }
              },
              "aggs": {
                "by_sns_id": {
                  "terms": {
                    "field": "published_by.sns_id",
                    "size": 0
                  }
                }
            }
        }

        # get total hit from elasticsearch
        res = client.search(index='rm_searching_staging',doc_type='m',body=query_count_keyword)
        return res['aggregations']['by_sns_id']['buckets']

    print  "Counting message containing keyword [" + keyword + "] from " ,days[0], " to " ,days[1], " from ES..."


    file_name = "count_msg_from_" + str(days[0]) + "_to_" + str(days[1]) + "[" + keyword + "]" +".csv"
    with open(file_name,'w') as fr:
        f = unicodecsv.writer(fr,encoding='UTF-8')

        #get total count per sns_id
        count_result = {}
        for hit in exc_query():
            count_result[hit['key']] = hit['doc_count']

        #write headline
        f.writerow(('sns_id','num_msg'))

        #write data
        for sns_id, total_count in enumerate(count_result):
            f.writerow((sns_id,total_count))

    return count_result



    print "All done! Please check the result file!"

    # return count_result
    return count_result

# count number of message by keyword from begin date to end date
def count_msg_by_keyword_from_to():

     year,month,date = raw_input("From date:").split()
     # #get begin date
     days = []
     days.append(DateTime.date(int(year),int(month),int(date)))
     #
     # #get end date
     year,month,date = raw_input("To date:").split()
     days.append(DateTime.date(int(year),int(month),int(date)))

     #get keyword
     keyword = str(raw_input("keyword:"))

     # get data and write result csv file
     count_msg_keyword(keyword,days)


    # file_name = "count_msg_from_" + str(days[0]) + "_to_" + str(days[1]) + "_[" + keyword + "]" +".csv"

def count_msg(days):

     year,month,date = raw_input("From date:").split()
     # #get begin date
     days = []
     days.append(DateTime.date(int(year),int(month),int(date)))
     #
     # #get end date
     year,month,date = raw_input("To date:").split()
     days.append(DateTime.date(int(year),int(month),int(date)))

     # get data and write result csv file
     count_msg_from_to(days)

def main():
     #check valid time
     invalid_time = True

     while(invalid_time==True):
         year,month,date = raw_input("From date:").split()
         # #get begin date
         days = []
         days.append(DateTime.date(int(year),int(month),int(date)))
         #
         # #get end date
         year,month,date = raw_input("To date:").split()
         days.append(DateTime.date(int(year),int(month),int(date)))
         #check valid time
         if (days[0] > days[1]):
             print "invalid time!!!"
         else:
             invalid_time = False


     #check valid keyword
     invalid_keyword = True
     #get keyword
     while(invalid_keyword==True):
         keyword = str(raw_input("keyword:"))
         if not keyword:
             print "Null keyword!!!!"
         else:
             invalid_keyword = False

     #call function query here


if __name__ == '__main__':
    main()