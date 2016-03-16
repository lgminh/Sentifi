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


def query_ES():
    def exec_query():
        query_event_tag={
            "fields": [
                "_id", "content_s", "title_s"
            ],
            "query":{
                "filtered":{
                    "filter":{
                        "bool":{
                            "must":[
                                {
                                    "range": {
                                        "published_at": {
                                            "from": "2016-1-1",
                                            "to": "2016-1-31"
                                        }
                                    }
                                },
                                {
                                    "term": {
                                        "channel": "news"
                                    }
                                },
                                {
                                  "term": {
                                    "lang": "de"
                                  }
                                },
                                {
                                  "term": {
                                    "is_sim": "false"
                                  }
                                }
                            ]
                        }
                    }
                }
            }
            # "filter": {
            #     "query": {
            #         "query_string":{
            #             "default_field": "les",
            #             "query": "Executives NOT is_sim:true"
            #         }
            #
            #     }
            # }
        }
        # return client.search(
        #     index='analytic',
        #     doc_type='relevant_document',
        #     body=query_event_tag,
        #     fields = ['_id','text']
        # )
        return helpers.scan(client, query=query_event_tag, index='rm_search_staging', doc_type='m')

    print "retrieve message"
    mDict = dict()

    # with open('test.csv', encoding='utf-8') as f:
    #     fw = unicodecsv.writer(f)
    #     fw.writerow(())

    with open('OneMonthsDE_news_1_16.csv', 'w') as fi:
        f = unicodecsv.writer(fi, encoding='utf-8')
        for hit in exec_query():#['hits']['hits']:
            # print hit
            # print hit['fields']['_id'], hit['fields']['text'][0]
            # print hit['_id']
            # mDict[hit['fields']['_id']]=hit['fields']['text'][0]
            id = hit['fields']['_id']
            title = hit['fields']['title_s'][0] if 'title_s' in hit['fields'] else ""
            text = hit['fields']['content_s'][0] if 'content_s' in hit['fields'] else ""
            f.writerow((id, title + '. ' + text))
    return mDict

def query_content_tag():
    def exec_query():
        query_msg_tag={
              "fields": [
                "content","ne_mentions.name"
              ],
               "query": {
                    "filtered": {
                      "filter": {
                        "bool": {
                          "must": [
                            {
                              "query": {
                                "query_string": {
                                  "default_field": "ne_classes",
                                  "query": "ListedCompanies"
                                }
                              }
                            },
                            {
                              "term": {
                                "lang": "en"
                              }
                            },
                            {
                              "term": {
                                "score_content": 0
                              }
                            },
                            {
                              "range": {
                                "created_at": {
                                  "from": "now-1w",
                                  "to": "now"
                                }
                              }
                            },
                            {
                              "term": {
                                "is_sim": "false"
                              }
                            }
                          ]
                        }
                      }
                    }
                  }
        }
        return helpers.scan(client, query=query_msg_tag, index='rm_search_staging', doc_type='m')

    print "retrieve message"
    mDict = dict()

    # with open('test.csv', encoding='utf-8') as f:
    #     fw = unicodecsv.writer(f)
    #     fw.writerow(())

    with open('listed_companies_score_zero_01_26.csv', 'w') as fi:
        f = unicodecsv.writer(fi, encoding='utf-8')
        f.writerow(('id', 'text', 'ne'))
        for hit in exec_query():#['hits']['hits']:
            # print hit
            # print hit['fields']['_id'], hit['fields']['text'][0]
            # print hit['_id']
            # mDict[hit['fields']['_id']]=hit['fields']['text'][0]
            id = hit['_id']
            ne_name = hit['fields']['ne_mentions.name'][0] if 'ne_mentions.name' in hit['fields'] else ""
            text = hit['fields']['content'][0] if 'content' in hit['fields'] else ""
            f.writerow((id, text, ne_name))
    return mDict

def query_msg_from_pub(sns_id):
    def exec_query():
        query_msg_pub={
        "fields": [
            "content"
          ],
          "query": {
            "filtered": {
              "filter": {
                "bool": {
                  "must": [
                    {
                      "query": {
                        "match": {
                          "published_by.sns_id": int(sns_id)
                        }
                      }
                    }
                  ]
                }
              }
            }
          }
        }
        return helpers.scan(client, query=query_msg_pub, index='rm_search_staging', doc_type='m')

    print "retrieve message"
    mDict = dict()

    for hit in exec_query():#['hits']['hits']:
        # print hit
        # print hit['fields']['_id'], hit['fields']['text'][0]
        # print hit['_id']
        # mDict[hit['fields']['_id']]=hit['fields']['text'][0]
        id = hit['_id']
        # ne_name = hit['fields']['ne_mentions.name'][0] if 'ne_mentions.name' in hit['fields'] else ""
        text = hit['fields']['content'][0] if 'content' in hit['fields'] else ""
        mDict[id] = text
    return mDict


def remove_messages(ids, msg_repo=None, producer=None, event_id = 0):
    for idx, mid in enumerate(ids):
        if not mid:
            continue
        try:
            msg = msg_repo.find_by_id(mid)
            if msg:

                msg_repo.update(
                    {
                        '_id': msg['_id']
                    },
                    {
                        'patterns': delete_patterns(msg['patterns'], event_id)
                    }
                )
                producer.publish(str(msg['_id']))
                print mid, idx
                # print "patterns ", delete_patterns(msg['patterns'], 118)
        except:
            print "ERROR:", mid, idx


def load_ids(csv_file=None, ignore_headline=True):
    import unicodecsv as csv
    with open(csv_file) as f:
        cur = csv.reader(f)
        if ignore_headline:
            cur.next()
        for r in cur:
            yield r[0], r[1:]

def delete_patterns(patterns, event_id):
    for id, impact in enumerate(patterns):
        pats = impact['pattern'] #list
        impact_group = str(impact['tag']) + '_'
        for idx, pat in enumerate(pats):
            if pat['id'] == event_id:
                if 'impact_group' in pat:
                    impact_group = impact_group + pat['impact_group']
                del pats[idx]
                break
        impact['pattern'] = pats
        if 'impact_tp' in impact and impact['impact_tp']:
            impact_tp = impact['impact_tp']
            for idx, tp in enumerate(impact_tp):
                if impact_group in tp :
                    del impact_tp[idx]
                    break
            impact['impact_tp'] = impact_tp
        patterns[id] = impact
    return patterns


def main():
    # msg_repo = MongoRepo(connect('mongoctrinh.ssh.sentifi.com'))
    # msg_repo.use('analytic', 'relevant_document')
    # BROKER_URL = [
    #     'amqp://worker:123cayngaycaydem@rabbitmq-prod2.ssh.sentifi.com:5672/',
    #     'amqp://worker:123cayngaycaydem@rabbitmq-prod1.ssh.sentifi.com:5672/'
    # ]
    # producer = MessageProducer.fanout(BROKER_URL, 'RelevantDocumentReady')
    #
    # event_id = [2293, 2294, 2295]
    # for e_id in event_id:
    #     ids = query_ES(e_id)
    #     print '****************************** processing ids:', len(ids), 'event_id: ', e_id
    #     remove_messages(ids, msg_repo, producer, e_id)

    # mDict = query_content_tag()
    # df = pd.DataFrame(columns=['id','text'])
    # for k,v in mDict.items():
    #     row = [k,v]
    #     df.loc[len(df)]=row
    # df.to_csv('0509_30dmessages.csv', sep=',', encoding='utf-8')
    with open('pub_msg_01_28.csv', 'w') as fw:
        writer = unicodecsv.writer(fw, encoding='utf-8')
        writer.writerow(('sns_id', 'msg_id', 'text'))
        # with open('/home/semantic/Downloads/pub_profile.csv','r') as fr:
        with open('pub_profile.csv','r') as fr:
            f= unicodecsv.reader(fr, encoding='utf-8')
            row = f.next()
            count = 0
            for row in f:
                count = count + 1
                print count, row[0]
                mDict = query_msg_from_pub(row[0])
                # print mDict
                for k,v in mDict.iteritems():
                    writer.writerow((row[0], k, v))

    print 'DONE!'

if __name__ == '__main__':
    # main()
    query_ES()