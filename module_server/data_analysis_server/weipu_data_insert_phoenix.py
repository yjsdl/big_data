# -*- coding: utf-8 -*-
# @date：2025/4/27 18:05
# @Author：LiuYiJie
# @file： weipu_data_insert_phoenix


from typing import Union, List
from module_server.data_analysis_server.phoenix_operate import phoenixServer
from module_server.data_analysis_server.util.school_article_from_mongo import exportIncrementData
from module_server.utils.log import logger


class weiPuDataInsertPhoenix:
    def __init__(self, filter_year: bool = True, query_field: str = None, query_value: str = None,
                 year: Union[int, List[int]] = None):
        self._filter_year = filter_year
        self._query_field = query_field
        self._query_value = query_value
        self._years = year
        self._phoenix_server = phoenixServer(logger=logger)

        self._export_increment_data = exportIncrementData(
            query_field=query_field,
            query_value=query_value,
            year=year
        )
        self._col = (
            'id', 'third_id', 'title', 'author', 'org', 'fund', 'journal', 'year', 'volum', 'issue', 'issn', 'cn',
            'page', 'keyword', 'classification_code', 'abstract', 'url', 'school_name', 'school_id')

    def article_insert_phoenix(self):
        article_gen = self._export_increment_data.export_article()
        for article_dict in article_gen:
            success_data = []
            error_data = []
            article_id_list = article_dict.get('article_id_list', [])
            article_list = article_dict.get('article_list', [])
            year = article_dict.get('year', '')
            for i in range(0, len(article_list), 1000):
                article_split_list = article_list[i:i+1000]

                sql = """upsert into SCIENCE.RAW_WEIPU_ARTICLE_METADATA values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
                # 批量提交1000条，如果出错，一条一条提交
                data_state = self._phoenix_server.upsert_many(sql, article_split_list)

                if data_state:
                    success_data.extend(article_id_list[i: i+1000])
                else:
                    for article_tuple in article_split_list:
                        data_state = self._phoenix_server.upsert_one(sql, article_tuple)
                        if data_state:
                            success_data.append(article_tuple[1])
                        else:
                            error_data.append(article_tuple[1])
            logger.info(
                f'当前学校{self._query_value} - {year}年 - 成功{len(success_data)} - 失败{len(error_data)}')
            logger.info(f'失败id为 - {error_data}')


if __name__ == '__main__':
    weiPuDataInsertPhoenix(
        query_field='school_name',
        query_value='南京师范大学',
        year=list(range(1990, 2026))
    ).article_insert_phoenix()