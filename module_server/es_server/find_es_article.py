# -*- coding: utf-8 -*-
# @date：2024/8/9 9:24
# @Author：LiuYiJie
# @file： find_es_article
import json
from elasticsearch import Elasticsearch
from hbase_server.phoenix_operate import phoenixServer

ES_CONF = {
    "host": "101.43.232.153",
    "port": "9200",
    "user": "elastic",
    "passwd": "kcidea1509!%)("
}


class findEsArticle:
    def __init__(self, es_index=None):
        self.es = Elasticsearch(
            [ES_CONF['host']],
            http_auth=(ES_CONF['user'], ES_CONF['passwd']),
            port=ES_CONF['port']
        )
        self.es_index = es_index
        # 初始化phoenix
        self.phoenix = phoenixServer()

    def search_article(self):
        query_json = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "school_ids": {
                                    "value": "51"
                                }
                            }
                        },
                        {
                            "term": {
                                "source_type": {
                                    "value": 6
                                }
                            }
                        }
                    ]
                }
            },
            "from": 0,
            "size": 1
        }
        results = self.es.search(index=self.es_index, body=query_json)['hits']['hits']
        data = [(str(result['_source']['id']), json.dumps(result['_source'], ensure_ascii=False), '2024-08-16') for result in results]

        # for result in results:
        #     id = str(result['_source']['id'])
        #     data = json.dumps(result['_source'])
        #     update_time = '2024-08-13'
        #
        #     # sql = """upsert into SCIENCE.SCIENCE_ARTICLE_METADATA(id, C1."article_msg",C1."update_time") values('%s','%s','%s')""" % (id, data, update_time)
        #     print(result)
        #     data = [
        #         ('111111', 'test1', '2024-08-13'),
        #         ('222222', 'test2', '2024-08-14'),
        #     ]
        sql = """upsert into SCIENCE.SCIENCE_ARTICLE_METADATA(id, C1."article_msg",C1."update_time") values(?,?,?)"""
        self.phoenix.upsert(sql, data)
        self.phoenix.pool.close_all()


if __name__ == '__main__':
    c = findEsArticle(es_index='science-article-metadata-v3')
    c.search_article()
