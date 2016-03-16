__author__ = 'duydo'

# from common.mongo import MongoRepo, connect
# from common.messaging import MessageProducer
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
import unicodecsv

ES_HOST = [
    {'host': 'es0.ssh.sentifi.com', 'port': 9200, 'timeout': 120 },
]
client = Elasticsearch(ES_HOST)

def query_impact_from_tag(OTags, MTags, LTags):
    def exec_query():
        query_tag_event={
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "published_at": {
                                    "from": "now-2d",
                                    "to": "now-1d"
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "by_events": {
                    "nested": {
                        "path":"events"
                    },
                    "aggs": {
                        "tag": {
                            "terms": {
                                "field": "events.ne_id",
                                "size": 0
                            },
                            "aggs": {
                                "eventID": {
                                    "terms": {
                                        "field": "events.impact_id",
                                        "size": 0
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        # print query_tag_event
        res = client.search(index='rm_search', body=query_tag_event,  search_type='count')
        return res['aggregations']['by_events']['tag']['buckets']

    print "retrieve tag and event"
    event_tag = exec_query()
    # print 'Num of Markets Msg', MarketsMsgNum
    OM_count = 0
    MM_count = 0
    LM_count = 0
    OE_count = 0
    ME_count = 0
    LE_count = 0
    for te in event_tag:
        if int(te['key']) in OTags:
            OM_count += te['doc_count']
            OE_count += len(te['eventID']['buckets'])
        elif int(te['key']) in MTags:
            MM_count += te['doc_count']
            ME_count += len(te['eventID']['buckets'])
        elif int(te['key']) in LTags:
            LM_count += te['doc_count']
            LE_count += len(te['eventID']['buckets'])

    # print 'Num of Markets Event', event_count
    return [OM_count, MM_count, LM_count, OE_count, ME_count, LE_count]

def query_event_from_tag(OTags, MTags, LTags):
    def exec_query():
        query_tag_event={
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "published_at": {
                                    "from": "now-2d",
                                    "to": "now-1d"
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "by_events": {
                    "nested": {
                        "path":"events"
                    },
                    "aggs": {
                        "tag": {
                            "terms": {
                                "field": "events.ne_id",
                                "size": 0
                            },
                            "aggs": {
                                "eventID": {
                                    "terms": {
                                        "field": "events.id",
                                        "size": 0
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        # print query_tag_event
        res = client.search(index='rm_search', body=query_tag_event,  search_type='count')
        return res['aggregations']['by_events']['tag']['buckets']

    print "retrieve tag and event"
    event_tag = exec_query()
    # print 'Num of Markets Msg', MarketsMsgNum
    OM_count = 0
    MM_count = 0
    LM_count = 0
    OE_count = 0
    ME_count = 0
    LE_count = 0
    for te in event_tag:
        if int(te['key']) in OTags:
            OM_count += te['doc_count']
            OE_count += len(te['eventID']['buckets'])
        elif int(te['key']) in MTags:
            MM_count += te['doc_count']
            ME_count += len(te['eventID']['buckets'])
        elif int(te['key']) in LTags:
            LM_count += te['doc_count']
            LE_count += len(te['eventID']['buckets'])

    # print 'Num of Markets Event', event_count
    return [OM_count, MM_count, LM_count, OE_count, ME_count, LE_count]

def query_tag_from_cat():
    def exec_query():
        query_cat_tag={
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "published_at": {
                                    "from": "now-2d",
                                    "to": "now-1d"
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "by_categories": {
                    "nested": {
                        "path":"ne_mentions"
                    },
                    "aggs": {
                        "cat": {
                            "terms": {
                                "field": "ne_mentions.category_id_paths",
                                "size": 0
                            },
                            "aggs": {
                                "tag": {
                                    "terms": {
                                        "field": "ne_mentions.id",
                                        "size": 0
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        res = client.search(index='rm_search', body=query_cat_tag,  search_type='count')
        return res['aggregations']['by_categories']['cat']['buckets']


    print "retrieve cat and tag"
    cat_tag = exec_query()
    # print cat_tag
    OrgTag = set()
    MarketsTag = set()
    LegislationTag = set()
    for hit in cat_tag:
        # print hit
        if hit['key'] == 2:
            for tag in hit['tag']['buckets']:
                OrgTag.add(tag['key'])
        elif hit['key'] == 8:
            for tag in hit['tag']['buckets']:
                MarketsTag.add(tag['key'])
        elif hit['key'] == 60000000:
            for tag in hit['tag']['buckets']:
                LegislationTag.add(tag['key'])
    return OrgTag, MarketsTag, LegislationTag

def count_num_msg(ne_ids, days):
    def exec_query():
        query_count={
          "query": {
            "bool": {
              "must": [
                {
                  "terms": {
                    "tag": ne_ids
                  }
                },
                {
                  "range": {
                    "published_at": {
                      "from": days[0],
                        "to": days[1]
                    }
                  }
                }
              ]
            }
          },
          "aggs": {
            "by_messages": {
              "terms": {
                "field":"tag",
                "size": 0
              }
            }
          }
        }
        res = client.search(index='rm_search_staging', body=query_count,  search_type='count')
        return res['aggregations']['by_messages']['buckets']
    print 'counting message from ES...', days
    count_result = {}
    for hit in exec_query():
        if hit['key'] in ne_ids:
            count_result[hit['key']]=hit['doc_count']
    return count_result

def count_pub_msg(month):
    def exec_query():
        query_count={
          "query": {
            "range": {
              "published_at": {
                "from": "2015-%s-1"%(month),
                  "to": "2015-%s-1"%(month + 1)
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
        res = client.search(index='rm_search_staging', body=query_count,  search_type='count')
        return res['aggregations']['by_sns_id']['buckets']
    print 'counting message from ES...', month
    count_result = {}
    for hit in exec_query():
        count_result[hit['key']]=hit['doc_count']
    return count_result
    # with open("pub_msg_month_%s.csv"%(month), 'w') as fi:
    #     f = unicodecsv.writer(fi, encoding='utf-8')
    #     f.writerow(('sns_id', 'msg_num'))
    #     for hit in exec_query():
    #         f.writerow((hit['key'],hit['doc_count']))
    print 'done'

def main():

    OrgTag, MarketsTag, LegislationTag = query_tag_from_cat()
    event = query_event_from_tag(OrgTag, MarketsTag, LegislationTag)
    impact = query_impact_from_tag(OrgTag, MarketsTag, LegislationTag)
    print 'event', event, event[0]+event[1]+event[2], event[3]+event[4]+event[5]
    print 'impact', impact, impact[0]+impact[1]+impact[2], impact[3]+impact[4]+impact[5]
    print 'DONE!'

def buzz():
    ne_ids = [16905,33229,34509,9098,15240,487,17372,323,311,9118,346,746168,305,7375,7383,746159,9638,746164,9813,317,8788,315,746161,9205,9206,319,9457,746156,10037,10535,11199,14325,15996,15102,15268,15403,16165,16822,16908,16910,17874,18216,18211,18291,22153,24335,23021,342,27018,30377,2015,405,2099,2095,2196,1999,2130,2088,2078,2059,2139,1791,746233,2073,2076,2114,1692,403,408,400,1460,2118,2167,402,406,396,2080,2112,2013,2154,2062,2108,397,2058,2103,1667,2128,413,2003,2000,2005,2006,2016,2020,90,2017,1277,2034,2036,2037]
    # ne_ids = [16905,33229]
    time_frames = [('2015-09-21T04:00:00', '2015-09-22T04:00:00'),('2015-09-28T04:00:00', '2015-09-29T04:00:00'), ('2015-10-05T04:00:00', '2015-10-06T04:00:00'),('2015-10-12T04:00:00', '2015-10-13T04:00:00'), ('2015-10-19T04:00:00', '2015-10-20T04:00:00'),('2015-10-26T04:00:00', '2015-10-27T04:00:00'),('2015-11-02T04:00:00', '2015-11-03T04:00:00'),('2015-11-09T04:00:00', '2015-11-10T04:00:00'), ('2015-11-16T04:00:00', '2015-11-17T04:00:00'),('2015-11-23T04:00:00', '2015-11-24T04:00:00'), ('2015-11-30T04:00:00', '2015-12-01T04:00:00'),('2015-12-07T04:00:00', '2015-12-08T04:00:00')]
    print 'time_frame', len(time_frames)
    weekly_count = {}
    allweek_count = {}
    for time_frame in time_frames:
        print 'processing...', time_frame
        weekly_count = count_num_msg(ne_ids, time_frame)
        for k,v in weekly_count.iteritems():
            new_value = v + allweek_count[k] if k in allweek_count else v
            allweek_count[k] = new_value
    with open('buzz_24h_12w.csv', 'w') as fi:
        f = unicodecsv.writer(fi, encoding='utf-8')
        f.writerow(('ne_id', '24h', 'avg_12w'))
        for id in ne_ids:
            _24h = weekly_count[id] if id in weekly_count else 0
            avg_12w = float(allweek_count[id])/len(time_frames) if id in allweek_count else 0
            f.writerow((id, _24h, avg_12w))

def pub_his():
    final_result = {}
    month_list = [1,2,3,4,5,6,7,8,9,10,11]
    # month_list = [1,2]
    for i in month_list:
        count_result = count_pub_msg(i)
        for k,v in count_result.iteritems():
            w_l = final_result[k] if k in final_result else {}
            w_l[i] = v
            final_result[k]=w_l
    with open("pub_msg_month.csv", 'w') as fi:
        f = unicodecsv.writer(fi, encoding='utf-8')
        f.writerow(('sns_id', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 'month_8', 'month_9', 'month_10', 'month_11'))
        for k,v in final_result.iteritems():
            row = [k]
            for i in month_list:
                num = v[i] if i in v else 0
                row.append(num)
            f.writerow(tuple(row))

if __name__ == '__main__':
    # main1()
    pub_his()
    # buzz()
