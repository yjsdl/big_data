# -*- coding: utf-8 -*-
# @date：2025/1/7 13:45
# @Author：LiuYiJie
# @file： school_article_from_mongo

from datetime import datetime
import hashlib
from pymongo import MongoClient
from module_server.utils.log import logger
from typing import List, Union
import os
import json
# from module_server.data_analysis_server.phoenix_operate import phoenixServer


filename = r'Z:\需求文档\代码\science-data-upload\science_data_upload\_base\school_sid.json'
with open(filename, 'r', encoding='utf-8') as f:
    school_sid = json.load(f)

school_alias_name_dict = {
    "清华大学深圳国际研究生院": {"$in": ["清华大学深圳", "清华大学深圳国际研究生院"]},
    "桂林医科大学": {"$in": ["桂林医科大学", "桂林医学院"]},
    "湖南中医药大学": {"$in": ["湖南中医药大学", "湖南中医学院"]},
    "河北中医药大学": {"$in": ["河北中医药大学", "河北中医学院"]},
    "河北工业职业技术大学": {"$in": ["河北工业职业技术大学", "河北工业职业技术学院"]},
}


class exportIncrementData:
    def __init__(self, filter_year: bool = True, query_value: str = None, from_db: str = 'data_weipu_article',
                 query_field: str = None, collection: str = 'relation_school_weipu',
                 year: Union[int, List[int]] = None):
        """
        :param filter_year: 是否过滤年份
        :param query_value:
        :param from_db:
        :param query_field:
        :param collection:
        :param year:
        """
        self._filter_year = filter_year
        self._query_value = query_value
        self._from_db = from_db
        self._years = year
        self._client = None
        self._db = 'science2'
        self._query_field = query_field
        self._collection = collection
        self._school_id = school_sid.get(query_value)
        if not self._school_id:
            raise ValueError('学校没有id, 请检查')
        # 特殊处理学校
        self._query_value_result = school_alias_name_dict.get(query_value, query_value)
        self._columns = {'title': '题名', 'author': '作者', 'org': '机构', 'fund': '基金', 'journal': '刊名', 'year': '年',
                         'volum': '卷',
                         'issue': '期', 'issn': 'ISSN号', 'cn': 'CN号', 'page': '页码', 'keyword': '关键词',
                         'classification_code': '分类号',
                         'abstract': '文摘', 'url': '网址'}
        # self._phoenix_server = phoenixServer(logger=logger)

    @property
    def client(self):
        if self._client is None:
            self._client = MongoClient(
                "mongodb://science-dev:kcidea1509!%25)(@101.43.232.153:27017/?authSource=science&directConnection=true")
        return self._client

    @property
    def db(self):
        return self.client[self._db]

    @staticmethod
    def id_md5(data):
        data = data.encode(encoding='utf-8')
        res = hashlib.new('md5', data)
        result = res.hexdigest()
        return result

    def export_school_all_variation(self, year: int = None):
        """
        """
        pipeline = [
            {
                '$match': {
                    self._query_field: self._query_value_result,
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

        if self._filter_year:
            pipeline.append({
                '$match': {
                    'article_detail.exported.year': year
                }
            })

        collection = self.db.get_collection(self._collection)
        cursor = collection.aggregate(pipeline)
        article_id_list = []
        success_data = []
        error_data = []
        result_list = []

        for data in cursor:
            customer_info = data.get('article_detail')
            for article in customer_info:
                third_id = article.get('third_id')
                article_id_list.append(third_id)
                article_data = article.get('exported')
                article_data['volum'] = str(article_data["volum"])
                article_data_list = list(article_data.values())
                id_md5 = self.id_md5(third_id + self._query_value)
                article_data_list.insert(0, id_md5)
                article_data_list.insert(1, third_id)
                article_data_list.append(self._query_value)
                article_data_list.append(self._school_id)
                article_data_list.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                article_data_tuple = tuple(article_data_list)
                result_list.append(article_data_tuple)
        logger.info(
            f'当前学校- {self._query_value}-{self._school_id} - {year} - 年 - 导出发文- {len(article_id_list)} -条')
        return {'article_id_list': article_id_list, 'article_list': result_list, 'year': year}

        # for i in range(0, len(result_list), 1000):
        #     article_list = result_list[i:i+1000]
        #
        #     sql = """upsert into SCIENCE.RAW_WEIPU_ARTICLE_METADATA values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        #     # 批量提交1000条，如果出错，一条一条提交
        #     data_state = self._phoenix_server.upsert_many(sql, article_list)
        #
        #     if data_state:
        #         success_data.extend(data_id_list[i: i+1000])
        #     else:
        #         for article_tuple in article_list:
        #             data_state = self._phoenix_server.upsert_one(sql, article_tuple)
        #             if data_state:
        #                 success_data.append(article_tuple[1])
        #             else:
        #                 error_data.append(article_tuple[1])
        # logger.info(
        #     f'当前学校{self._query_value} - {year}年 - 导出发文{len(data_id_list)}条 - '
        #     f'成功{len(success_data)} - 失败{len(error_data)}')
        # logger.info(f'失败id为 - {error_data}')

    def export_article(self):
        if self._years and isinstance(self._years, list):
            for year in self._years:
                yield self.export_school_all_variation(year)
        else:
            yield self.export_article()
        self.client.close()
        logger.info('执行结束, 关闭mongo数据库连接')
        # self._phoenix_server.pool.close_all()


if __name__ == '__main__':
    # c = exportIncrementData(
    #     # collection='relation_subject_weipu',
    #     # 是否按年份过滤
    #     query_field='school_name',
    #     query_value='桂林医科大学',
    #     year=list(range(1990, 2026))
    # )
    # c.export_article()
    logger.info('测试')
    logger.error('测试')
    logger.warning('测试')