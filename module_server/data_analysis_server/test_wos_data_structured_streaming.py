# -*- coding: utf-8 -*-
# @date：2025/8/22 10:35
# @Author：LiuYiJie
# @file： test_wos_data_structured_streaming
"""
从kafka中读取wos数据，经过处理存储到phoenix
"""
import re
import json
import bson
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, IntegerType, StructField, StructType, MapType
from pyspark.sql.functions import md5, udf, col, split, explode, from_json, collect_list, struct, pandas_udf
from module_server.utils.log import logger
import pandas as pd
from module_server.data_analysis_server.util.utils import handle_format_str, replace_semicolon_outside_brackets


# 策略缓存管理器
class StrategyManager:

    def __init__(self):
        self._include_strategy = None
        self._exclude_strategy = None
        self._last_refresh = 0
        self.REFRESH_INTERVAL = 300  # 5分钟刷新
        self._mysql_db = 'science'
        self._mysql_url = f"jdbc:mysql://43.140.203.187:3306/{self._mysql_db}?characterEncoding=UTF-8&connectionCollation=utf8mb4_general_ci&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=GMT%2b8"

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
        df = spark.read.format('jdbc') \
            .option('url', self._mysql_url) \
            .option('query', table_query) \
            .option('user', 'science-read') \
            .option('password', 'readkcidea')\
            .load()
        logger.info('------------------------------------------学校排除策略---------------------------------------------')
        # 学校排除策略
        dic_not_school_dict = (
            df.fillna({'condition2': ''}).groupBy("school_id").agg(
                collect_list(struct("condition1", "condition2")).alias("conditions"))
                .rdd
                .map(lambda row: (row.school_id, [{"condition1": c.condition1, "condition2": c.condition2} for c in row.conditions]))
                .collectAsMap()
        )

        return dic_not_school_dict


        # dic_not_school_list = df.collect()
        # print(dic_not_school_list)
        # return dic_not_school_list

    def ai_dic_school_from_mysql(self):
        table_query = f"""
             select school_id, condition1, condition2 from ai_dic_school 
        """
        df = spark.read.format('jdbc') \
            .option('url', self._mysql_url) \
            .option('query', table_query) \
            .option('user', 'science-read') \
            .option('password', 'readkcidea') \
            .load()
        logger.info('------------------------------------------学校判断策略---------------------------------------------')
        # 学校判断策略
        dic_school_dict = (
            df.fillna({'condition2': ''}).groupBy("school_id").agg(
                collect_list(struct("condition1", "condition2")).alias("conditions"))
                .rdd
                .map(lambda row: (row.school_id, [{"condition1": c.condition1, "condition2": c.condition2} for c in row.conditions]))
                .collectAsMap()
        )
        print(dic_school_dict)
        return dic_school_dict

    def get_strategies(self):
        current_time = time.time()
        if not self._include_strategy or (current_time - self._last_refresh) > self.REFRESH_INTERVAL:
            logger.info('开始更新判断策略和排除策略')
            # 从MySQL加载策略
            self._include_strategy = self.ai_dic_school_from_mysql()
            self._exclude_strategy = self.ai_dic_not_school_from_mysql()
            self._last_refresh = current_time

        return self._include_strategy, self._exclude_strategy


# # 策略判断UDF（广播变量优化）
# def check_strategy_udf(content, initial_school_id):
#     include_rules, exclude_rules = StrategyManager.get_strategies(spark)
#
#     # 1. 应用排除策略（优先）
#     for rule in exclude_rules:
#         if evaluate_rule(content, rule):  # 规则评估函数
#             return False
#
#     # 2. 应用包含策略
#     for rule in include_rules:
#         if evaluate_rule(content, rule):
#             return True
#
#     # 3. 默认返回初始学校
#     return True
#
#
# # 注册UDF
# check_strategy = udf(check_strategy_udf, BooleanType())


# @pandas_udf(StringType())
# def process_author_pandas_udf(author_raw):
#     def process_single_string(author_str):
#         if pd.isna(author_str):
#             return author_str
#         return ';'.join([handle_format_str(author_name_str) for author_name_str in author_str.split(';')])
#
#     return author_raw.apply(process_single_string)


# 定义返回类型 - MapType(StringType(), IntegerType())
# 表示键是字符串，值是整数


def process_author_address(row):
    author_order_list_raw: str = row['author_order_list_raw']
    author_order_list = json.loads(author_order_list_raw)

    author_address_raw_str = row['author_address_raw']
    author_address_result_list = []
    try:
        author_address_replace_str = replace_semicolon_outside_brackets(author_address_raw_str)
        author_address_list = author_address_replace_str.split('\x00')
        author_address_standard_list = [author_address_str.replace('\x00', ';') for author_address_str in
                                        author_address_list]

        # 没有作者只有地址, 将地址归到所有作者上
        if ('[' not in author_address_raw_str) and len(author_address_standard_list) == 1:
            for index, author_address_one_str in enumerate(author_address_standard_list, start=1):
                author_address_standard_one_str = handle_format_str(author_address_one_str)

                for author_order_dict in author_order_list:
                    author_order_dict['author_address'] = author_address_one_str.strip()
                    author_order_dict['author_address_standard'] = author_address_standard_one_str
                    author_order_dict['author_address_order'] = index
                    author_address_result_list.append(author_order_dict.copy())

        else:
            for index, author_address_one_str in enumerate(author_address_standard_list, start=1):
                # 如果有 ] 代表地址中有作者，需要将作者地址对应
                if ']' in author_address_one_str:
                    # 作者列表
                    author_format_list = [handle_format_str(author_str) for author_str in author_address_one_str.split(']')[0].split(';')]
                    # 作者地址
                    author_address_one_raw = author_address_one_str.split(']')[1]
                    author_address_standard_one_str = handle_format_str(author_address_one_raw)
                    # 判断作者是否在地址里面
                    for author_format_one_str in author_format_list:
                        for author_order_dict in author_order_list:
                            if author_format_one_str in (author_order_dict['author_name'], author_order_dict['author_alias']):
                                author_order_dict['author_address'] = author_address_one_raw.strip()
                                author_order_dict['author_address_standard'] = author_address_standard_one_str
                                author_order_dict['author_address_order'] = index
                                author_address_result_list.append(author_order_dict.copy())
                else:
                    # 发文地址中没有作者信息
                    author_address_dict = {}
                    author_address_standard_one_str = handle_format_str(author_address_one_str)
                    author_address_dict.update({"author_name": "", "author_order": '', "author_alias": "",
                                                "author_rep_order": ''})

                    author_address_dict['author_address'] = author_address_one_str.strip()
                    author_address_dict['author_address_standard'] = author_address_standard_one_str
                    author_address_dict['author_address_order'] = index
                    author_address_result_list.append(author_address_dict)
    except Exception as e:
        logger.warning('作者地址 %s 处理出错，错误原因为 %s' % (author_address_raw_str, e))

    author_address_result_to_str = json.dumps(author_address_result_list, ensure_ascii=False)

    return author_address_result_to_str


@pandas_udf(StringType())
def process_author_address_pandas_udf(author_order_list_raw, author_address_raw):
    """
    处理作者和地址关系
    :param author_order_list_raw: 发文作者顺位
    :param author_address_raw: 发文地址
    :return:
    """
    pdf = pd.DataFrame({
        "author_order_list_raw": author_order_list_raw,
        "author_address_raw": author_address_raw,
    })

    return pdf.apply(process_author_address, axis=1)


def process_author_row(row: pd.Series) -> dict:
    author_raw = row["author_raw"]
    author_abb_raw = row["author_abb_raw"]
    rep_author_raw = row["rep_author_raw"]

    # 处理通讯作者
    if not rep_author_raw:
        rep_author_dict = dict()
        rep_author_list = ''
        rep_author_address_list = ''
    else:
        rep_author_dict = dict()
        rep_author_list = []
        rep_author_address_list = []
        try:
            author_num = 1
            author_address_num = 1
            for rep_author in rep_author_raw.split('.;'):
                # 通讯作者
                for author_name in rep_author.split('(')[0].split(';'):
                    author_name_str = handle_format_str(author_name)
                    if author_name_str not in rep_author_dict.keys():
                        rep_author_dict[author_name_str] = author_num
                        rep_author_list.append(dict(rep_author_name=author_name_str, rep_author_order=author_num))
                        author_num += 1
                # 通讯地址
                rep_author_address = handle_format_str(rep_author.split(')，')[1])
                rep_author_address_list.append(dict(rep_author_address=rep_author_address,
                                                    rep_author_address_order=author_address_num))
                author_address_num += 1
        except Exception as e:
            logger.warning('通讯作者 %s 处理出错，错误原因为 %s' % (rep_author_raw, e))

    # 处理作者
    author_sort_list = []
    author_abb_to_full_dict = {}  # 作者简称对应到全称
    if not author_raw:
        author_sort_list = ''
    else:
        author_list = [handle_format_str(author_name_str) for author_name_str in author_raw.split(';')]
        author_abb_list = [handle_format_str(author_name_str) for author_name_str in author_abb_raw.split(';')]
        for index, author_name in enumerate(author_list, start=1):
            author_abb_to_full_dict[author_abb_list[index - 1]] = author_name

            author_sort_dict = {'author_name': author_name, 'author_order': index,
                                'author_alias': author_abb_list[index - 1],
                                'author_rep_order': rep_author_dict.get(author_abb_list[index - 1], 0)}
            author_sort_list.append(author_sort_dict)

    # 将通讯作者简称映射到全称去
    rep_author_full_list = [{"rep_author_name": author_abb_to_full_dict.get(rep_author_dict['rep_author_name'],
                                                                            rep_author_dict['rep_author_name']),
                             "rep_author_order": rep_author_dict['rep_author_order']}
                            for rep_author_dict in rep_author_list]

    author_sort_to_str = json.dumps(author_sort_list, ensure_ascii=False)
    rep_author_to_str = json.dumps(rep_author_full_list, ensure_ascii=False)
    rep_author_address_to_str = json.dumps(rep_author_address_list, ensure_ascii=False)

    return {
        "author_order": author_sort_to_str,
        "rep_author_order": rep_author_to_str,
        "rep_author_address_order": rep_author_address_to_str
    }


author_schema = StructType([
    StructField("author_order", StringType()),
    StructField("rep_author_order", StringType()),
    StructField("rep_author_address_order", StringType())

])


@pandas_udf(author_schema)
def process_author_pandas_udf(author_raw, author_abb_raw, rep_author_raw):
    """
    :param author_raw:作者全称
    :param author_abb_raw:作者简称
    :param rep_author_raw:通讯作者
    :return:
    """
    pdf = pd.DataFrame({
        "author_raw": author_raw,
        "author_abb_raw": author_abb_raw,
        "rep_author_raw": rep_author_raw
    })
    return pdf.apply(process_author_row, axis=1, result_type="expand")


class WosDataAnalysis:
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
    # @staticmethod
    # def relation_author_address(author_str, address_str):
    #     if not address_str or not author_str:
    #         return ''
    #     author_list = author_str.split(';')
    #     author_order_list = []
    #     author_id_dict = {}
    #
    #     address_list = address_str.split(';')
    #     address_order_list = []
    #     address_tag_dict = {}
    #     address_id_dict = {}
    #
    #     author_address_list = []
    #     for address in address_list:
    #         address_tag = address.split(']')[0].strip(*['。 ['])
    #         address_order = address_list.index(address) + 1
    #         try:
    #             address_name = address.split(']')[1].strip('')
    #
    #         except IndexError:
    #             address_name = ''
    #         address_tag_dict[address_tag] = address_name
    #         address_id_dict[address_name] = str(bson.ObjectId())
    #         address_order_list.append({'id': address_id_dict.get(address_name, ''), 'authorName': address_name,
    #                                    'number': address_order, 'repNumber': '', 'repTag': '', 'sourceType': 3})
    #
    #     for author in author_list:
    #         author_name = author.split('[')[0].strip(*['。 '])
    #         author_id_dict[author_name] = str(bson.ObjectId())
    #
    #         author_order = author_list.index(author) + 1
    #         # 处理一些名字中的脏数据
    #         # author_name = replace_special_char(author_name)
    #         try:
    #             author_nums = author.split('[')[1].strip(*['] 。'])
    #             author_order_list.append(
    #                 {'id': author_id_dict.get(author_name, ''), 'authorName': author_name, 'number': author_order,
    #                  'sourceType': 3, 'repNumber': '', 'email': ''})
    #         except IndexError:
    #             author_nums = ''
    #         if ',' in author_nums:
    #             author_num_list = author_nums.split(',')
    #             for author_num in author_num_list:
    #                 address_name = address_tag_dict.get(author_num, '')
    #                 author_address_list.append({'authorId': author_id_dict.get(author_name, ''), 'authorName': author_name,
    #                                             'addressId': address_id_dict.get(address_name, ''),
    #                                             'authorAddress': address_name, 'repTag': ''})
    #         else:
    #             address_name = address_tag_dict.get(author_nums, '')
    #             author_address_list.append({'authorId': author_id_dict.get(author_name, ''), 'authorName': author_name,
    #                                         'addressId': address_id_dict.get(address_name, ''),
    #                                         'authorAddress': address_name, 'repTag': ''})
    #
    #     return [json.dumps(author_address_list, ensure_ascii=False),
    #             json.dumps(author_order_list, ensure_ascii=False), json.dumps(address_order_list, ensure_ascii=False)]
    #
    # @staticmethod
    # def handle_format_str(keyword_str):
    #     keyword_id_list = []
    #     keyword_str = keyword_str.strip(*[';；%'])
    #     keyword_list = re.split("[;；%]", keyword_str)
    #     keyword_lists = [re.sub(r"(\[.*?\])", "", one) for one in keyword_list]
    #     for keyword in keyword_lists:
    #         keyword_id_list.append({'id': str(bson.ObjectId()), 'name': keyword, 'remark': ''})
    #
    #     return json.dumps(keyword_id_list, ensure_ascii=False)

    # 定义Pandas UDF


    # @property
    # def strategy_manager(self):
    #     if self._strategy_manager is None:
    #         self._strategy_manager = StrategyManager()
    #     return self._strategy_manager



    # def article_data_analysis(self, batch_df, batch_id):
    def article_data_analysis(self):

        """
        从kafka中读取数据，并进行作者，地址清洗
        :return:
        """
        table_query_format = f"""
        SELECT *
        FROM SCIENCE.TEST_DATA_WOS_ARTICLE
        limit 50
        """
        batch_df = spark.read \
            .format("jdbc") \
            .option("url", self._zookeeper_url) \
            .option("dbtable", 'SCIENCE.TEST_DATA_WOS_ARTICLE') \
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
            .load()


        try:
            print("-------------------------------------------hbase维普原始数据-------------------------------------------")
            batch_df1 = batch_df.limit(50)

            # 处理作者和通讯地址排序
            df_handle_author_standard = batch_df1.withColumn('author_standard', process_author_pandas_udf(col('AF'), col('AU'), col('RP')))
            df_author_order = df_handle_author_standard.select("*", "author_standard.*").drop("author_standard")

            # 处理地址
            df_handle_author_address_standard = df_author_order.withColumn('author_address_order',
                                                                           process_author_address_pandas_udf(
                                                                               col('author_order'), col('C1')))
            df_handle_author_address_standard.show(truncate=False)

        except Exception as e:
            print(f"[Batch {'batch_id'}] 错误: {e}")

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
            StructField("ID", StringType(), True),
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
            .trigger(processingTime="10 seconds")\
            .option("checkpointLocation", "hdfs://hadoop01:8020/spark_kafka/ch")\
            .start() \
            .awaitTermination()




if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('test wos data structured streaming'). \
        master('local[*]'). \
        getOrCreate()
    WosDataAnalysis().article_data_analysis()
    spark.catalog.clearCache()
    spark.stop()