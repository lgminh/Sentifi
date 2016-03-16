__author__ = 'minhloc'
# from common.mongo import MongoRepo, connect
# from common.messaging import MessageProducer
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
import unicodecsv
import datetime as DateTime

ES_HOST = [
    {'host': 'es0.ssh.sentifi.com', 'port': 9200, 'timeout': 120 },
]
client = Elasticsearch(ES_HOST)

# count number of message by keyword from begin date to end date
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
        res = client.search(index='rm_searching_staging', body=query_count_keyword, doc_type='m')
        return res['aggregations']['by_sns_id']['buckets']

    print  "Counting message containing keyword [" + keyword + "] from " ,days[0], " to " ,days[1], " from ES..."

    #write data
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
        for sns_id, total_count in enumerate(sorted(count_result)):
            f.writerow((sns_id,total_count))

    return count_result



    print "All done! Please check the result csv file!"

    # return count_result
    return count_result

def count_msg_by_keyword_from_to():

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




     # get data and write result csv file
     count_msg_keyword(keyword,days)

def count_multi_keyword(keywords,days):
    time = {
            "range": {
                "published_at": {
                  "from": str(days[0]),
                  "to": str(days[1])
                    }
                }
            }

    keywords_days = keywords.append(time)

    def exc_query():
        query_count_keyword = {
             "query": {
                "bool": {
                  "must": keywords_days
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
        res = client.search(index='rm_searching_staging', body=query_count_keyword, doc_type='m')
        return res['aggregations']['by_sns_id']['buckets']

    print  "Counting message containing keywords [" + keywords + "] from " ,days[0], " to " ,days[1], " from ES..."

def count_msg_by_multi_keyword_from_to():

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


     keywords = []
     #check valid keyword
     invalid_keyword = True
     #get keyword
     while(invalid_keyword==True):
         keyword = str(raw_input("keyword:"))
         if not keyword:
             break
         else:
             word = {"query":{
                        "query_string": {
                          "default_field": "ne_classes",
                          "query": keyword
                                        }
                            }
                    }
             keywords.append(word)

     print keywords
     # get data and write result csv file
     #count_multi_keyword(keywords, days)

def main():
     option = raw_input(
               "Count msg from ... to ... with a keyword --> press '1'\n"
               "Count msg from ... to ... with many keywords --> press '2'\n")

     if int(option) == 1:
        count_msg_by_keyword_from_to()
     elif int(option) == 2:
        count_msg_by_multi_keyword_from_to()
     else:
        print "Not a available option!!!"



if __name__ == '__main__':
     main()