# -*- coding: utf-8 -*-
# @date：2025/4/25 15:32
# @Author：LiuYiJie
# @file： weipu_data_insert_kafka
"""
从mongo中读取数据，并插入到kafka中
"""
from module_server.data_analysis_server.util.school_article_from_mongo import exportIncrementData
from module_server.data_analysis_server.util.kafka_producer_async import kafkaProducerAsync
from typing import Union, List


class weiPuDataInsertKafka:
    def __init__(self, filter_year: bool = True, query_field: str = None, query_value: str = None,
                 year: Union[int, List[int]] = None):
        self._filter_year = filter_year
        self._query_field = query_field
        self._query_value = query_value
        self._years = year

        self._export_increment_data = exportIncrementData(
            query_field=query_field,
            query_value=query_value,
            year=year
        )
        self._kafka_producer = kafkaProducerAsync(topic='testTopic')
        self._col = (
            'id', 'third_id', 'title', 'author', 'org', 'fund', 'journal', 'year', 'volum', 'issue', 'issn', 'cn',
            'page', 'keyword', 'classification_code', 'abstract', 'url', 'school_name', 'school_id')

    def article_insert_kafka(self):
        article_gen = self._export_increment_data.export_article()
        for article_dict in article_gen:
            article_id_list = article_dict.get('article_id_list', [])
            article_list = article_dict.get('article_list', [])
            # 将数据转为json格式
            article_json_list = [dict(zip(self._col, article)) for article in article_list][10:20]
            # 发送到kafka
            self._kafka_producer.send_data(article_json_list)


if __name__ == '__main__':
    weiPuDataInsertKafka(
        query_field='school_name',
        query_value='清华大学深圳国际研究生院',
        year=list(range(2024, 2025))
    ).article_insert_kafka()
