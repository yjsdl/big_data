# -*- coding: utf-8 -*-
# @date：2025/1/7 13:45
# @Author：LiuYiJie
# @file： school_article_to_hbase

from datetime import datetime
from pymongo import MongoClient
# from hbase_server.log_code.log import logger
from data_analysis_server.log_code.log import setup_logger
from typing import List, Union
import os
import json
import data_analysis_server.setting as log_setting
from hbase_server.phoenix_operate import phoenixServer


filename = r'Z:\需求文档\代码\science-data-upload\science_data_upload\_base\school_sid.json'
with open(filename, 'r', encoding='utf-8') as f:
    school_sid = json.load(f)


class exportIncrementData:
    def __init__(self, output_path: str = None, first: bool = True, single_school: bool = False,
                 query_value: str = None, start_time=None, end_time=datetime.now(), from_db: str = 'data_weipu_article',
                 query_field: str = 'school_name',
                 collection: str = 'relation_school_weipu', year: Union[int, List[int]] = None):
        """
        :param first:
        """
        self._first = first
        self._output_path = output_path
        self._single_school = single_school
        self._query_value = query_value
        self._start_time = start_time
        self._end_time = end_time
        self._from_db = from_db
        self._year = year
        self._client = None
        self._db = 'science2'
        self._query_field = query_field
        self._collection = collection
        self._school_id = school_sid.get(query_value)
        if not self._school_id:
            raise ValueError('学校没有id, 请检查')
        self._columns = {'title': '题名', 'author': '作者', 'org': '机构', 'fund': '基金', 'journal': '刊名', 'year': '年',
                         'volum': '卷',
                         'issue': '期', 'issn': 'ISSN号', 'cn': 'CN号', 'page': '页码', 'keyword': '关键词',
                         'classification_code': '分类号',
                         'abstract': '文摘', 'url': '网址'}
        self._phoenix_server = phoenixServer()

    @property
    def client(self):
        if self._client is None:
            self._client = MongoClient(
                "mongodb://science-dev:kcidea1509!%25)(@101.43.232.153:27017/?authSource=science&directConnection=true")
        return self._client

    @property
    def db(self):
        return self.client[self._db]

    @property
    def output_path(self):
        self._output_path = os.path.join(self._output_path, "output_file")
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)
        return self._output_path

    def find_download_ids(self) -> (list, list):
        pipeline = [
            {
                '$lookup': {
                    'from': self._from_db,  # 外部表
                    'localField': 'third_id',  # 本表条件
                    'foreignField': 'third_id',
                    'as': 'customer_info'  # 外部表数据字段名
                }
            }
        ]
        collection = self.db.get_collection('todo_ids_weipu')
        cursor = collection.aggregate(pipeline)
        data_articles = []
        not_exist_ids = []

        for data in cursor:
            customer_info = data.get('customer_info')
            # 如果存在customer_info，库里有当前数据
            if customer_info:
                if len(customer_info) > 1:
                    logger.info('当前维普id:%s存在多个数量' % data.get('third_id'))
                else:
                    customer_info = customer_info[0]
                    export = customer_info.get('exported')
                    data_articles.append(export)
            else:
                not_exist_ids.append({'third_id': data.get('third_id')})
        return data_articles, not_exist_ids

    def export_school_all_variation(self, filter_year: int = None):
        """
        """
        pipeline = [
            {
                '$match': {
                    self._query_field: self._query_value,
                },
            },
            {
                '$lookup': {
                    'from': self._from_db,
                    'localField': "third_id",
                    'foreignField': "third_id",
                    'as': "article_detail",
                },
            },
            {
                '$project': {
                    'title': 0,
                    'updated_at': 0,
                    "article_detail._id": 0,
                    "article_detail.updated_at": 0,
                },
            },
        ]

        if filter_year != 1111:
            pipeline.append({
                '$match': {
                    'article_detail.exported.year': filter_year
                }
            })

        collection = self.db.get_collection(self._collection)
        cursor = collection.aggregate(pipeline)
        data_list = []
        success_data = []
        error_data = []

        for data in cursor:
            customer_info = data.get('article_detail')
            for article in customer_info:
                third_id = article.get('third_id')
                data_list.append(third_id)
                article_data = article.get('exported')
                article_data['volum'] = str(article_data["volum"])
                article_data_list = list(article_data.values())
                article_data_list.insert(0, third_id)
                article_data_list.append(self._school_id)
                article_data_tuple = tuple(article_data_list)
                sql = """upsert into SCIENCE.SCIENCE_ARTICLE_METADATA values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
                data_state = self._phoenix_server.upsert_one(sql, article_data_tuple)
                if data_state:
                    success_data.append(third_id)
                else:
                    error_data.append(third_id)
        logger.info(
            f'当前学校{self._query_value} - {filter_year}年 - 导出发文{len(data_list)}条 - '
            f'成功{len(success_data)} - 失败{len(error_data)}')
        logger.info(f'失败id为 - {error_data}')

    def export_article(self, filter_year: int = 1111):
        self.export_school_all_variation(filter_year)

    def run(self):
        if self._single_school:
            if self._year and isinstance(self._year, list):
                for year in self._year:
                    self.export_article(year)
            elif self._year and isinstance(self._year, int):
                self.export_article(self._year)
            else:
                self.export_article()
        self.client.close()
        self._phoenix_server.pool.close_all()


def update_setting():
    setattr(log_setting, "LOG_NAME", "school_article_to_hbase")
    setattr(log_setting, "LOG_TO_FILE", True)


if __name__ == '__main__':
    # 郑州轻工业大学、北京理工大学、中国人民大学、北京工业大学、北京科技大学、郑州大学、齐鲁工业大学、 哈尔滨工程大学、东北师范大学
    update_setting()
    logger = setup_logger()
    c = exportIncrementData(
        # True表示单个学校，默认False
        single_school=True,
        # collection='relation_subject_weipu',
        query_field='school_name',
        query_value='西安建筑科技大学',
        year=list(range(2000, 2024))
    )
    c.run()