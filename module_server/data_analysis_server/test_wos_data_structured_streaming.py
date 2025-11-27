# -*- coding: utf-8 -*-
# @date：2025/8/22 10:35
# @Author：LiuYiJie
# @file： test_wos_data_structured_streaming
"""
从kafka中读取wos数据，经过处理存储到phoenix
"""
import math
import re
import json
from datetime import datetime, timedelta
import bson
import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, BooleanType
from pyspark.sql.functions import col, from_json, collect_list, struct, pandas_udf, \
    regexp_extract, date_sub, current_timestamp
from functools import reduce
from module_server.utils.log import logger
import pandas as pd
from module_server.data_analysis_server.util.utils import handle_format_str, replace_semicolon_outside_brackets, \
    get_month_str, month_str_dict


# 策略缓存管理器
class strategyManager:

    def __init__(self):
        self._include_strategy_dict = None
        self._exclude_strategy_dict = None
        self._last_refresh = 0
        self.REFRESH_INTERVAL = 300  # 5分钟刷新
        self._mysql_db = 'science'
        self._mysql_url = f"jdbc:mysql://43.140.203.187:3306/{self._mysql_db}?characterEncoding=UTF-8&connectionCollation=utf8mb4_general_ci&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=GMT%2b8"
        self._broadcast_school_strategy_dict = None

    @staticmethod
    def get_ai_dic_school_strategy_dict(df: DataFrame):
        dic_school_dict = (
            df.fillna({'condition2': ''}).groupBy("school_id").agg(
                collect_list(struct("condition1", "condition2")).alias("conditions")).rdd.map(lambda row: (
                    row.school_id,
                    [{"condition1": c.condition1, "condition2": c.condition2} for c in row.conditions])).collectAsMap()
        )
        return dic_school_dict

    def obtain_school_strategy(self, query_name: str = "MYSQL", params_type: str = "query",
                               table_or_sql_query: str = None, retry_num: int = 3, retry_interval: int = 5):
        """
        查询数据库，失败时递归重试。
        :param query_name: 查询名称
        :param params_type: 参数类型, 查询整个表或者根据sql查询
        :param table_or_sql_query: SQL 查询语句
        :param retry_num: 剩余重试次数
        :param retry_interval: 重试间隔秒数
        :return: Spark DataFrame
        """
        try:
            logger.info(f"正在执行 {query_name} 查询, 查询判断策略和排除策略（剩余重试次数：{retry_num}）")
            df: DataFrame = spark.read.format('jdbc') \
                .option('url', self._mysql_url) \
                .option(params_type, table_or_sql_query) \
                .option('user', 'science-read') \
                .option('password', 'readkcidea') \
                .load()
            # 需要过滤的学校
            filter_school = ["51", "156", "212", "211", "49", "213", "217"]
            df_specific_school = df.filter(col("school_id").isin(filter_school))
            return df_specific_school
        except Exception as e:
            logger.warning(f"{table_or_sql_query} | 查询失败: {e}, 准备重试")
            if retry_num > 1:
                time.sleep(retry_interval)
                # 递归调用，重试次数减一
                return self.obtain_school_strategy(query_name=query_name, params_type=params_type,
                                                   table_or_sql_query=table_or_sql_query, retry_num=retry_num - 1,
                                                   retry_interval=retry_interval)
            else:
                print(f"sql查询失败，已重试 {retry_num} 次，放弃重试")
                return spark.createDataFrame([], schema=StructType([]))

    def ai_dic_not_school_from_mysql(self):
        table_query = f"""
            (
            SELECT adns.school_id, ads.condition1, ads.condition2
            FROM ai_dic_school ads
            JOIN ai_dic_not_school adns
            ON ads.school_name = adns.not_school_name
            )
            UNION ALL
            (
            SELECT school_id, condition1, condition2
            FROM ai_dic_not_school
            WHERE (condition1 IS NOT NULL OR condition2 IS NOT NULL )
            )
        """

        logger.info('------------------------------学校排除策略------------------------------')
        # 学校排除策略
        df_specific_school = self.obtain_school_strategy(table_or_sql_query=table_query)
        dic_not_school_dict = self.get_ai_dic_school_strategy_dict(df_specific_school)
        print(dic_not_school_dict)
        return dic_not_school_dict

    def ai_dic_school_from_mysql(self):
        table_query = f"""
             select school_id, condition1, condition2 from ai_dic_school
        """
        logger.info('------------------------------学校判断策略------------------------------')
        # 学校判断策略
        df_specific_school = self.obtain_school_strategy(table_or_sql_query=table_query)
        dic_school_dict = self.get_ai_dic_school_strategy_dict(df_specific_school)
        print(dic_school_dict)
        return dic_school_dict

    def get_strategies(self):
        """
        获取学校判断策略和排除策略
        :return:
        """
        # current_time = time.time()
        # print(not self._include_strategy_dict)
        # print(current_time - self._last_refresh, self.REFRESH_INTERVAL)
        # print((current_time - self._last_refresh) > self.REFRESH_INTERVAL)
        # if not self._include_strategy_dict or ((current_time - self._last_refresh) > self.REFRESH_INTERVAL):

        logger.info('------------------------------开始更新判断策略和排除策略------------------------------')
        # 从MySQL加载策略
        self._include_strategy_dict = self.ai_dic_school_from_mysql()
        self._exclude_strategy_dict = self.ai_dic_not_school_from_mysql()
        # self._last_refresh = current_time

        return self._include_strategy_dict, self._exclude_strategy_dict

    def refresh_broadcast_school_strategy(self):
        """
        刷新广播变量
        :return:
        """
        current_time = time.time()

        if not self._broadcast_school_strategy_dict or ((current_time - self._last_refresh) > self.REFRESH_INTERVAL):
            ai_dic_school_dict, ai_dic_not_school_dict = self.get_strategies()
            school_strategy_dict = dict(
                ai_dic_school_dict=ai_dic_school_dict,
                ai_dic_not_school_dict=ai_dic_not_school_dict
            )
            # 如果存在旧的广播变量, 释放缓存, 重新生成
            if self._broadcast_school_strategy_dict:
                self._broadcast_school_strategy_dict.unpersist()
            self._broadcast_school_strategy_dict = spark.sparkContext.broadcast(school_strategy_dict)
            self._last_refresh = current_time

    def process_relation_school_udf(self):
        """
        根据判断策略和排除策略，处理学校关系
        :return: False or True
        """
        # 如果没有广播变量, 重新获取
        # if not self._broadcast_school_strategy_dict:
        #     print('获取判断策略和排除策略广播变量', self._broadcast_school_strategy_dict)

        self.refresh_broadcast_school_strategy()

        broadcast_school_strategy_dict = self._broadcast_school_strategy_dict

        @pandas_udf(BooleanType())
        def process_relation_school_pandas_udf(third_id, school_id, address_raw):
            """
            根据判断策略和排除策略，处理学校关系
            :param third_id: 第三方id
            :param school_id: 学校id
            :param address_raw: 原始发文地址
            :return:
            """
            broadcast_school_strategy_dict_value = broadcast_school_strategy_dict.value
            ai_dic_school_dict: dict = broadcast_school_strategy_dict_value.get('ai_dic_school_dict', {})
            ai_dic_not_school_list_dict: dict = broadcast_school_strategy_dict_value.get('ai_dic_not_school_dict', {})

            def filter_address(row):
                """
                排除不符合条件的学校
                :param row:
                :return: true: 不排除当前地址，false:排除当前地址
                """
                third_id_str = row['third_id']
                school_id_str = row['school_id']
                address_list_str_raw = row['address_raw']

                # 根据学校id获取当前学校的策略
                dic_school_list = ai_dic_school_dict.get(int(school_id_str), [])
                dic_not_school_list = ai_dic_not_school_list_dict.get(int(school_id_str), [])

                if not address_list_str_raw:
                    return False

                result_school_flag = False
                address_list_dict = json.loads(address_list_str_raw)
                for address_dict in address_list_dict:
                    address_str: str = address_dict.get('address_format', '')

                    # 1.地址经过判断策略验证，验证通过=进行下一步，验证不通过=非本校地址
                    is_school_flag = False
                    for ai_dic_school in dic_school_list:
                        condition1 = ai_dic_school['condition1']
                        condition2 = ai_dic_school['condition2']
                        if condition1 in address_str:
                            if condition2 is None:
                                is_school_flag = True
                            else:
                                is_school_flag = condition2 in address_str
                        if is_school_flag:
                            break

                    if is_school_flag:
                        # 1.地址经过判断策略，如果通过判断策略验证是本校地址，添加排除策略验证
                        # 如果地址包含了排除策略的内容，那就非本校地址
                        # 默认地址不通过排除策略验证
                        exclude_flag = False
                        for ai_dic_not_school in dic_not_school_list:
                            condition1 = ai_dic_not_school['condition1']
                            condition2 = ai_dic_not_school['condition2']
                            if condition1 in address_str:
                                if condition2 is None:
                                    exclude_flag = True
                                else:
                                    exclude_flag = condition2 in address_str
                            if exclude_flag:
                                break
                        if exclude_flag:
                            is_school_flag = False
                    # 如果找到一条属于本校, 直接跳出
                    if is_school_flag:
                        result_school_flag = True
                        break
                logger.debug(f'第三方ID: {third_id_str}, 学校- {school_id_str} - 处理完成')
                return result_school_flag

            pdf = pd.DataFrame({
                "third_id": third_id,
                "school_id": school_id,
                "address_raw": address_raw,
            })
            return pdf.apply(filter_address, axis=1)

        return process_relation_school_pandas_udf


def process_publication_date_across_moon(third_id, pub_year, pub_date, match_obj) -> list:
    """
    处理日期中月份跨月
    :param third_id: 第三方id
    :param pub_year: 出版年
    :param pub_date: 出版日期
    :param match_obj: 日期正则对象
    :return:
    """
    date_list = []
    try:
        date_result = [match_obj.group(1), match_obj.group(2)]
        if len(date_result) != 2:
            logger.warning("月份范围解析失败 - 第三方ID: {}, 日期字符串: {}", third_id, pub_date)
            return date_list

        start_month = get_month_str(date_result[0])
        end_month = get_month_str((date_result[1]))
        if start_month == 0 or end_month == 0:
            # 月份转换失败，降级处理：只保存年份信息
            logger.warning("月份转换失败，降级为年份记录 - 第三方ID: {}, 起始月: {}, 结束月: {}", third_id, date_result[0], date_result[1])
            date_list.append(dict(pub_year=pub_year, v_month=0, v_day=0))
            return date_list

        # 生成月份范围内的所有日期记录
        current_month = start_month
        loop_counter = 0
        max_iterations = 13

        while loop_counter < max_iterations:
            date_list.append(dict(pub_year=pub_year, v_month=current_month, v_day=0))

            # 检查是否到达结束月份
            if current_month == end_month:
                break
            current_month += 1
            # 处理跨年情况：12月之后回到1月（如NOV-FEB的情况）
            if current_month > 12:
                current_month = 1

            # 情况1：正常范围（如MAR-APR，3-4月）
            if start_month <= end_month < current_month:
                break
            # 情况2：跨年范围（如NOV-FEB，11月-2月）
            if start_month > end_month and end_month < current_month < start_month:
                break
            loop_counter += 1
            logger.debug("月份范围解析完成 - 第三方ID: {}, 范围: {}-{}, 生成记录数: {}", third_id, date_result[0], date_result[1],
                         len(date_list))
    except Exception as e:
        logger.warning('第三方ID: %s ,出版日期 %s 处理出错, 错误原因为 %s' % (third_id, pub_date, e))
    return date_list


def process_publication_date(row) -> str:
    """
    处理wos出版日期
    :param row: 行数据
    :return: 列表转换成str
    """
    third_id = row['third_id']
    pub_year = row['pub_year']
    pub_date = row['pub_date']
    date_list = []
    try:
        if not pub_date:
            date_list.append(dict(pub_year=pub_year, v_month=0, v_day=0))

        else:
            # 处理跨月
            simple_month_range_pattern = re.compile(r'^([A-Z]{3})-([A-Z]{3})$', re.IGNORECASE)
            match_obj = re.match(simple_month_range_pattern, pub_date)
            if '-' in pub_date and match_obj:
                date_list = process_publication_date_across_moon(third_id, pub_year, pub_date, match_obj)
            else:
                # wos的写法中有带年份的,比如：2023 DEC 12
                # FEB 20
                # MAR
                month = 0
                day = 0
                pub_date_list = pub_date.strip().split()
                # 解析普通写法
                for date_str in pub_date_list:
                    date_str_lower = date_str.lower()
                    if date_str_lower in month_str_dict:  # 匹配月份
                        month = get_month_str(date_str_lower)
                    elif re.fullmatch(r"\d{1,2}", date_str):  # 匹配日
                        day = date_str.zfill(2)

                date_list.append(dict(pub_year=pub_year, v_month=month, v_day=int(day)))

    except Exception as e:
        logger.warning('第三方ID: %s ,出版日期 %s 处理出错, 错误原因为 %s' % (third_id, pub_date, e))

    date_list_to_str = json.dumps(date_list, ensure_ascii=False)
    return date_list_to_str


@pandas_udf(StringType())
def process_publication_date_pandas_udf(third_id, pub_year, pub_date):
    """
    处理文章出版年份udf
    :param third_id: 发文id
    :param pub_date:
    :param pub_year:
    :return:
    """
    pdf = pd.DataFrame({
        "third_id": third_id,
        "pub_year": pub_year,
        "pub_date": pub_date,
    })
    return pdf.apply(process_publication_date, axis=1)


def process_author_address_row(third_id, author_raw, author_abb_raw, author_address_raw):
    """
    处理普通作者顺位，地址顺位，作者地址关系
    :param third_id: 第三方id
    :param author_raw: 作者全称原始数据
    :param author_abb_raw: 作者简称原始数据
    :param author_address_raw: 作者地址原始数据
    :return:
    """
    # 处理作者
    author_result_list = []
    # 地址字段
    address_result_list = []
    # 发文作者地址关系
    relation_author_address_list = []

    try:
        author_abb_to_full_dict = {}  # 作者简称对应到全称
        author_raw_list = author_raw.split(';')
        author_abb_raw_list = author_abb_raw.split(';')

        for index, author_name in enumerate(author_raw_list, start=1):
            # author_abb_to_full_dict[author_abb_list[index - 1]] = author_name
            author_name_format = handle_format_str(author_name)
            author_alias = author_abb_raw_list[index - 1]
            author_alias_format = handle_format_str(author_alias)
            author_id = str(bson.ObjectId())
            author_sort_dict = {
                'author_id': author_id, 'author_name': author_name, 'author_name_format': author_name_format,
                'author_number': index, 'author_alias': author_alias, 'author_alias_format': author_alias_format,
                'rep_author_number': 0,
            }
            author_result_list.append(author_sort_dict)

        author_address_replace_str = replace_semicolon_outside_brackets(author_address_raw)
        author_address_list = author_address_replace_str.split('\x00')
        author_address_standard_list = [author_address_str.replace('\x00', ';') for author_address_str in
                                        author_address_list]

        # 没有作者只有地址, 将地址归到所有作者上
        if ('[' not in author_address_raw) and len(author_address_standard_list) == 1:
            for index, address_one_str in enumerate(author_address_standard_list, start=1):
                address_one_format = handle_format_str(address_one_str)
                address_id = str(bson.ObjectId())

                address_dict = {'address_id': address_id, 'address': address_one_str.strip(),
                                'address_format': address_one_format, 'address_number': index,
                                'rep_address_number': 0}
                address_result_list.append(address_dict)

                for author_msg_dict in author_result_list:
                    author_msg_dict['address_id'] = address_id
                    relation_author_address_list.append(
                        dict(author_id=author_msg_dict['author_id'], address_id=address_id, rep_tag=0))

        else:
            for index, author_address_one_str in enumerate(author_address_standard_list, start=1):
                # 如果有 ] 代表地址中有作者，需要将作者地址对应
                if ']' in author_address_one_str:
                    # 作者列表
                    author_format_list = [handle_format_str(author_str) for author_str in
                                          author_address_one_str.split(']')[0].split(';')]
                    # 作者地址
                    author_address_one_raw = author_address_one_str.split(']')[1]
                    author_address_standard_one_str = handle_format_str(author_address_one_raw)
                    address_id = str(bson.ObjectId())

                    address_dict = {'address_id': address_id, 'address': author_address_one_raw.strip(),
                                    'address_format': author_address_standard_one_str, 'address_number': index,
                                    'rep_address_number': 0}
                    address_result_list.append(address_dict)
                    # 判断作者是否在地址里面
                    for author_format_one_str in author_format_list:
                        for author_msg_dict in author_result_list:
                            if author_format_one_str in (
                                    author_msg_dict['author_name_format'], author_msg_dict['author_alias_format']):
                                relation_author_address_list.append(
                                    dict(author_id=author_msg_dict['author_id'], address_id=address_id, rep_tag=0))

                else:
                    # 发文地址中没有作者信息
                    address_dict = {}
                    address_str_format = handle_format_str(author_address_one_str)
                    address_id = str(bson.ObjectId())
                    address_dict['address_id'] = address_id
                    address_dict['address'] = author_address_one_str.strip()
                    address_dict['address_format'] = address_str_format
                    address_dict['address_number'] = index
                    address_dict['rep_address_number'] = 0
                    address_result_list.append(address_dict)
    except Exception as e:
        logger.warning('第三方ID: %s ,作者地址 %s 处理出错, 错误原因为 %s' % (third_id, author_address_raw, e))

    return author_result_list, address_result_list, relation_author_address_list


def process_rep_author_address_row(third_id, rep_author_raw):
    """
    处理通讯作者顺位，地址顺位，作者地址关系
    :param third_id: 第三方id
    :param rep_author_raw: 通讯作者地址原始数据
    :return:
    """
    rep_author_list = []
    rep_address_list = []
    relation_rep_author_address_list = []
    logger.debug('')
    try:
        rep_author_dict = dict()
        rep_author_unique_id_dict = dict()
        rep_author_num = 1
        rep_address_num = 1
        for rep_author in rep_author_raw.split('.;'):
            # 先处理通讯地址，可能一个地址对应多个作者
            # 通讯地址
            rep_address_raw = rep_author.split(')，')[1]
            rep_address_format = handle_format_str(rep_address_raw)
            rep_address_id = str(bson.ObjectId())

            rep_address_list.append(dict(
                address_id=rep_address_id,
                address=rep_address_raw,
                address_format=rep_address_format,
                address_number=0,
                rep_address_number=rep_address_num))
            rep_address_num += 1

            # 通讯作者
            for rep_author_name in rep_author.split('(')[0].split(';'):
                author_name_format = handle_format_str(rep_author_name)
                rep_author_id = str(bson.ObjectId())

                # 如果作者相同, 先查找
                relation_rep_author_address_list.append(
                    dict(rep_author_id=rep_author_unique_id_dict.get(author_name_format, rep_author_id),
                         rep_address_id=rep_address_id, rep_tag=1))

                if author_name_format not in rep_author_dict.keys():
                    rep_author_dict[author_name_format] = rep_author_num
                    rep_author_unique_id_dict[author_name_format] = rep_author_id
                    rep_author_list.append(dict(
                        author_id=rep_author_id,
                        author_name=rep_author_name,
                        author_name_format=author_name_format,
                        author_number=0,
                        author_alias='',
                        author_alias_format='',
                        rep_author_number=rep_author_num
                    ))
                    rep_author_num += 1

    except Exception as e:
        if not rep_author_raw:
            logger.warning('第三方ID %s, 通讯作者 %s 处理出错, 错误原因为 %s' % (third_id, rep_author_raw, e))

    return rep_author_list, rep_address_list, relation_rep_author_address_list

    # # 将通讯作者简称映射到全称去
    # rep_author_full_list = [{"rep_author_name": author_abb_to_full_dict.get(rep_author_dict['rep_author_name'],
    #                                                                         rep_author_dict['rep_author_name']),
    #                          "rep_author_order": rep_author_dict['rep_author_order']}
    #                         for rep_author_dict in rep_author_list]


def process_author_address_relation_row(row):
    """
    处理作者，地址顺位，作者地址关系
    :param row: 行数据
    :return:
    """
    third_id = row['third_id']
    author_raw = row["author_raw"]
    author_abb_raw = row["author_abb_raw"]
    rep_author_raw = row["rep_author_raw"]
    author_address_raw = row["author_address_raw"]

    # 通讯作者
    rep_author_list, rep_address_list, relation_rep_author_address_list = process_rep_author_address_row(third_id,
                                                                                                         rep_author_raw)
    print(f"rep_author_list:{rep_author_list}")
    print(f"rep_address_list:{rep_address_list}")
    print(f"relation_rep_author_address_list:{relation_rep_author_address_list}")
    # 普通作者
    author_result_list, address_result_list, relation_author_address_list = process_author_address_row(
        third_id,
        author_raw,
        author_abb_raw,
        author_address_raw)

    print(f"author_result_list:{author_result_list}")
    print(f"address_result_list:{address_result_list}")
    print(f"relation_author_address_list:{relation_author_address_list}")


    result_author_list = [a.copy() for a in author_result_list]

    # TODO: 通讯作者和普通作者合并逻辑，后续处理
    # zjp:如果同一作者是通讯作者和普通作者，将通讯作者的id改为普通作者的id,
    # zjp:作者地址对应关系，只需要存两个字段，author_id，address_id

    for rep_author_dict in rep_author_list:
        rep_name_fmt = rep_author_dict.get("author_name_format")
        matched = False

        for author_dict in result_author_list:
            if rep_name_fmt in (author_dict.get("author_name_format"), author_dict.get("author_alias_format")):
                author_dict["rep_author_number"] = rep_author_dict.get("rep_author_number", 0)
                for relation_rep_author_address_dict in relation_rep_author_address_list:
                    if relation_rep_author_address_dict['rep_author_id'] == rep_author_dict['author_id']:
                        relation_rep_author_address_dict['rep_author_id'] = author_dict['author_id']
                # 将通讯作者地址关系里面
                matched = True
                break

        if not matched:  # 没匹配上就追加
            result_author_list.append(rep_author_dict.copy())

    result_address_list = address_result_list + rep_address_list
    result_relation_author_address_list = relation_author_address_list + relation_rep_author_address_list

    author_to_str = json.dumps(result_author_list, ensure_ascii=False)
    address_to_str = json.dumps(result_address_list, ensure_ascii=False)
    relation_author_address_to_str = json.dumps(result_relation_author_address_list, ensure_ascii=False)

    return {
        '"author_order"': author_to_str,
        '"address_order"': address_to_str,
        '"relation_author_address"': relation_author_address_to_str
    }


author_schema = StructType([
    StructField('"author_order"', StringType()),
    StructField('"address_order"', StringType()),
    StructField('"relation_author_address"', StringType())

])


@pandas_udf(author_schema)
def process_author_address_pandas_udf(third_id, author_raw, author_abb_raw, rep_author_raw, author_address_raw):
    """
    处理作者和地址顺位，关系 udf
    :param third_id: 发文id
    :param author_raw:作者全称
    :param author_abb_raw:作者简称
    :param rep_author_raw:通讯作者
    :param author_address_raw: 作者地址
    :return:
    """
    pdf = pd.DataFrame({
        "third_id": third_id,
        "author_raw": author_raw,
        "author_abb_raw": author_abb_raw,
        "rep_author_raw": rep_author_raw,
        "author_address_raw": author_address_raw
    })
    return pdf.apply(process_author_address_relation_row, axis=1, result_type="expand")


class wosDataAnalysis:
    def __init__(self):
        self._zookeeper_url = (
            "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181:hbase-unsecure;"
            "characterEncoding=UTF-8;"
            "phoenix.schema.isNamespaceMappingEnabled=true;"
        )
        # Kafka 配置
        self.kafka_bootstrap_servers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
        self.kafka_topic = "testWosTopic"
        self._strategy_manager = None

    @property
    def strategy_manager(self):
        if self._strategy_manager is None:
            self._strategy_manager = strategyManager()
        return self._strategy_manager

    def data_from_db(self, query_name: str = "SQL", params_type: str = "dbtable", table_or_sql_query: str = None,
                     retry_num: int = 3, retry_interval: int = 5):
        """
        Phoenix 查询数据库，失败时递归重试。
        :param query_name: 查询名称
        :param params_type: 参数类型, 查询整个表或者根据sql查询
        :param table_or_sql_query: SQL 查询语句
        :param retry_num: 剩余重试次数
        :param retry_interval: 重试间隔秒数
        :return: Spark DataFrame
        """
        try:
            logger.info(f"正在执行 {query_name} 查询（剩余重试次数：{retry_num}）")
            df: DataFrame = spark.read \
                .format("jdbc") \
                .option("url", self._zookeeper_url) \
                .option(params_type, table_or_sql_query) \
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
                .option("fetchsize", 5000) \
                .load()

            return df
        except Exception as e:
            logger.warning(f"{table_or_sql_query} | 查询失败: {e}, 准备重试")
            if retry_num > 1:
                time.sleep(retry_interval)
                # 递归调用，重试次数减一
                return self.data_from_db(query_name=query_name, params_type=params_type,
                                         table_or_sql_query=table_or_sql_query, retry_num=retry_num - 1,
                                         retry_interval=retry_interval)
            else:
                print(f"sql查询失败，已重试 {retry_num} 次，放弃重试")

                return spark.createDataFrame([], schema=StructType([]))

    def article_data_analysis(self, batch_df, batch_id):
    # def article_data_analysis(self):

        """
        从kafka中读取数据，并进行作者，地址清洗
        :return:
        """
        table_query_format = f"""
        SELECT *
        FROM SCIENCE.TEST_DATA_WOS_ARTICLE
        limit 50
        """
        # batch_df = self.data_from_db(query_name='wos原始数据', params_type="dbtable",
        #                              table_or_sql_query="SCIENCE.TEST_DATA_WOS_ARTICLE")

        try:
            print("------------------------------hbase维普原始数据------------------------------")
            # batch_df1 = batch_df.limit(5)
            batch_df1 = batch_df.filter(col("ID") == "WOS:001243765000001")

            # 处理作者和通讯地址
            df_handle_author_address = batch_df1. \
                withColumn('author_address_standard',
                           process_author_address_pandas_udf(col('ID'), col('AF'), col('AU'), col('RP'), col('C1')))

            df_author_address_format = df_handle_author_address. \
                select("*", "author_address_standard.*").drop("author_address_standard")

            # 处理年份
            df_handle_publication_date = df_author_address_format.\
                withColumn('"pub_date_format"', process_publication_date_pandas_udf(col('ID'), col('PY'), col('PD')))

            # 处理提前出版年份
            df_handle_early_publication = df_handle_publication_date.\
                withColumn('"early_date_format"', regexp_extract(col("EA"), r"(\d{4})", 1))

            df_handle_early_publication.show(truncate=False)
            print('执行第一次')

            # 处理发文关系
            self.data_to_db_and_school_relation(df_handle_early_publication)

        except Exception as e:
            print(f"[Batch {'batch_id'}] 错误: {e}")

    def data_to_db_and_school_relation(self, df_row: DataFrame):
        """
        将数据存入hbase
        根据学校的判断策略和排除策略，处理学校发文关系
        wos只处理特殊的几个学校
        对近三天的发文关系进行判断
        :param df_row:
        :return:
        """
        # # 将格式化后的数据缓存，后续做清洗
        # df_row.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        # 缓存两个字段
        df_id_address = df_row.select('ID', '"address_order"').persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

        logger.info(f"数据清洗完成, 准备入库")
        df_row_rename = df_row.withColumnRenamed("updated_time", '"updated_time"')
        df_row_rename.write.format('phoenix').mode('append') \
            .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
            .option("table", "SCIENCE.DATA_RESULT_WOS_ARTICLE") \
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
            .option("batchsize", 5000) \
            .save()
        print('执行第二次')
        # 根据第三方id查出初始的学校关系
        third_id_list = [row['ID'] for row in df_id_address.select('ID').distinct().collect()]

        # 处理发文关系
        self.obtain_relation_school_raw(third_id_list, df_id_address)
        logger.info('发文关系入库完成')
        df_id_address.unpersist()

    def obtain_relation_school_raw(self, third_id_list: list, df_row: DataFrame, batch_size: int = 5000):
        """
        :param df_row: id和地址表
        :param third_id_list: 需要过滤的 third_id 列表
        :param batch_size: 每批查询的 ID 数量，防止 SQL 过长
        :return:
        """

        start_time = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
        total_batches = math.ceil(len(third_id_list) / batch_size)
        df_all_batches = []
        for i in range(total_batches):
            batch_ids = third_id_list[i * batch_size:(i + 1) * batch_size]

            # 将ID拼接成 SQL IN (...)，记得加单引号
            batch_ids_str = ",".join(f"'{x}'" for x in batch_ids)

            table_query = f"""
                    SELECT "ID", "third_id", "school_id", "source_type", "updated_time"
                    FROM SCIENCE.RELATION_SCHOOL_ARTICLE
                    WHERE "updated_time" >= TO_TIMESTAMP('{start_time}')
                      AND "third_id" IN ({batch_ids_str})
                """

            df: DataFrame = self.data_from_db(query_name='wos发文关系', params_type="query", table_or_sql_query=table_query)
            df_all_batches.append(df)

        # 合并所有批次结果
        if df_all_batches:
            df_final_result = reduce(DataFrame.union, df_all_batches)
            df_final_result.show()

            df_join_address = df_final_result.join(df_row, df_final_result["third_id"] == df_row["ID"], "inner").\
                drop(df_row['ID'])

            # 判断策略和排除策略
            filter_school_udf = self.strategy_manager.process_relation_school_udf()

            df_school_filter = df_join_address.filter(
                filter_school_udf(col("third_id"), col("school_id"), col('"address_order"')))

            # 重命名所有列
            df_renamed = df_school_filter.select(
                col("ID"),
                col("third_id").alias('"third_id"'),
                col("school_id").alias('"school_id"'),
                col("source_type").alias('"source_type"'),
                col("updated_time").alias('"updated_time"')
            )

            df_renamed.write.format('phoenix').mode('append') \
                .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
                .option("table", "SCIENCE.RELATION_SCHOOL_ARTICLE_RESULT") \
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
                .option("batchsize", 5000) \
                .save()

    def read_article_from_kafka(self):
        # 读取 Kafka 数据
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .load()

        # 假设 Kafka 数据是 JSON，需要解析成 DataFrame
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("PT", StringType(), True),
            StructField("AU", StringType(), True),
            StructField("BA", StringType(), True),
            StructField("BE", StringType(), True),
            StructField("GP", StringType(), True),
            StructField("AF", StringType(), True),
            StructField("BF", StringType(), True),
            StructField("CA", StringType(), True),
            StructField("TI", StringType(), True),
            StructField("SO", StringType(), True),
            StructField("SE", StringType(), True),
            StructField("BS", StringType(), True),
            StructField("LA", StringType(), True),
            StructField("DT", StringType(), True),
            StructField("CT", StringType(), True),
            StructField("CY", StringType(), True),
            StructField("CL", StringType(), True),
            StructField("SP", StringType(), True),
            StructField("HO", StringType(), True),
            StructField("DE", StringType(), True),
            StructField("ID_ALIAS", StringType(), True),
            StructField("AB", StringType(), True),
            StructField("C1", StringType(), True),
            StructField("C3", StringType(), True),
            StructField("RP", StringType(), True),
            StructField("EM", StringType(), True),
            StructField("RI", StringType(), True),
            StructField("OI", StringType(), True),
            StructField("FU", StringType(), True),
            StructField("FP", StringType(), True),
            StructField("FX", StringType(), True),
            StructField("CR", StringType(), True),
            StructField("NR", StringType(), True),
            StructField("TC", StringType(), True),
            StructField("Z9", StringType(), True),
            StructField("U1", StringType(), True),
            StructField("U2", StringType(), True),
            StructField("PU", StringType(), True),
            StructField("PI", StringType(), True),
            StructField("PA", StringType(), True),
            StructField("SN", StringType(), True),
            StructField("EI", StringType(), True),
            StructField("BN", StringType(), True),
            StructField("J9", StringType(), True),
            StructField("JI", StringType(), True),
            StructField("PD", StringType(), True),
            StructField("PY", IntegerType(), True),
            StructField("VL", StringType(), True),
            StructField("IS", StringType(), True),
            StructField("PN", StringType(), True),
            StructField("SU", StringType(), True),
            StructField("SI", StringType(), True),
            StructField("MA", StringType(), True),
            StructField("BP", StringType(), True),
            StructField("EP", StringType(), True),
            StructField("AR", StringType(), True),
            StructField("DI", StringType(), True),
            StructField("DL", StringType(), True),
            StructField("D2", StringType(), True),
            StructField("EA", StringType(), True),
            StructField("PG", StringType(), True),
            StructField("WC", StringType(), True),
            StructField("WE", StringType(), True),
            StructField("SC", StringType(), True),
            StructField("GA", StringType(), True),
            StructField("PM", StringType(), True),
            StructField("OA", StringType(), True),
            StructField("HC", StringType(), True),
            StructField("HP", StringType(), True),
            StructField("DA", StringType(), True),
            StructField("updated_time", StringType(), True)
        ])

        # df_stream.selectExpr("CAST(value AS STRING)", 'timestamp').\
        #     writeStream.format('console').outputMode('append').start().awaitTermination()

        df_cast_value = df_stream.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        # 使用 foreachBatch 将数据写入 Phoenix
        df_cast_value.writeStream \
            .foreachBatch(self.article_data_analysis) \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .option("checkpointLocation", "hdfs://hadoop01:8020/spark_kafka/ch") \
            .start() \
            .awaitTermination()


if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('test wos data structured streaming'). \
        master('local[*]'). \
        getOrCreate()
    wosDataAnalysis().read_article_from_kafka()

    # wosDataAnalysis().article_data_analysis()
    # strategyManager().get_strategies()
    spark.catalog.clearCache()
    spark.stop()
