# -*- coding: utf-8 -*-
# @date：2025/2/20 15:08
# @Author：LiuYiJie
# @file： weipu_data_analysis
"""
解析hbase中的维普数据，生成标准化数据
"""
import re
import json
from bson import ObjectId
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import md5, udf, col, split, explode


class weipuDataAnalysis:
    def __init__(self, school_name: str = None, school_id: str = None):
        self._school_name = school_name
        self._school_id = school_id
        self._zookeeper_url = (
            "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181:hbase-unsecure;"
            "characterEncoding=UTF-8;"
            "phoenix.schema.isNamespaceMappingEnabled=true;"
        )
        self._mysql_db = 'science'
        self._mysql_url = f"jdbc:mysql://43.140.203.187:3306/{self._mysql_db}?characterEncoding=UTF-8&connectionCollation=utf8mb4_general_ci&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=GMT%2b8"

    @staticmethod
    def relation_author_address(author_str, address_str):
        if not address_str or not author_str:
            return ''
        author_list = author_str.split(';')
        author_order_list = []
        author_id_dict = {}

        address_list = address_str.split(';')
        address_order_list = []
        address_tag_dict = {}
        address_id_dict = {}

        author_address_list = []
        for address in address_list:
            address_tag = address.split(']')[0].strip(*['。 ['])
            address_order = address_list.index(address) + 1
            try:
                address_name = address.split(']')[1].strip('')

            except IndexError:
                address_name = ''
            address_tag_dict[address_tag] = address_name
            address_id_dict[address_name] = str(ObjectId())
            address_order_list.append({'id': address_id_dict.get(address_name, ''), 'authorName': address_name,
                                       'number': address_order, 'repNumber': '', 'repTag': '', 'sourceType': 3})

        for author in author_list:
            author_name = author.split('[')[0].strip(*['。 '])
            author_id_dict[author_name] = str(ObjectId())

            author_order = author_list.index(author) + 1
            # 处理一些名字中的脏数据
            # author_name = replace_special_char(author_name)
            try:
                author_nums = author.split('[')[1].strip(*['] 。'])
                author_order_list.append(
                    {'id': author_id_dict.get(author_name, ''), 'authorName': author_name, 'number': author_order,
                     'sourceType': 3, 'repNumber': '', 'email': ''})
            except IndexError:
                author_nums = ''
            if ',' in author_nums:
                author_num_list = author_nums.split(',')
                for author_num in author_num_list:
                    address_name = address_tag_dict.get(author_num, '')
                    author_address_list.append({'authorId': author_id_dict.get(author_name, ''), 'authorName': author_name,
                                                'addressId': address_id_dict.get(address_name, ''),
                                                'authorAddress': address_name, 'repTag': ''})
            else:
                address_name = address_tag_dict.get(author_nums, '')
                author_address_list.append({'authorId': author_id_dict.get(author_name, ''), 'authorName': author_name,
                                            'addressId': address_id_dict.get(address_name, ''),
                                            'authorAddress': address_name, 'repTag': ''})

        return [json.dumps(author_address_list, ensure_ascii=False),
                json.dumps(author_order_list, ensure_ascii=False), json.dumps(address_order_list, ensure_ascii=False)]

    @staticmethod
    def handle_keyword(keyword_str):
        keyword_id_list = []
        keyword_str = keyword_str.strip(*[';；%'])
        keyword_list = re.split("[;；%]", keyword_str)
        keyword_lists = [re.sub(r"(\[.*?\])", "", one) for one in keyword_list]
        for keyword in keyword_lists:
            keyword_id_list.append({'id': str(ObjectId()), 'name': keyword, 'remark': ''})

        return json.dumps(keyword_id_list, ensure_ascii=False)


    def obtain_data_analysis_from_hbase(self):
        """
        从hbase中读取数据，并进行作者，地址清洗
        :return:
        """
        # 使用 DataFrame 读取数据
        # TODO
        table_query_format = f"""
        SELECT ID, "third_id", "title", "author", "org", "fund", "journal", "year", "volum", "issue","issn", "cn", 
        "page", "keyword", "classification_code", "abstract", "url", "school_name", "school_id", "updated_time"
        FROM SCIENCE.RAW_WEIPU_ARTICLE_METADATA
        WHERE "school_id" = '{self._school_id}' 
        limit 50
        """
        # table_query = table_query_format.replace(":school_id", str(self._school_id))

        df = spark.read \
            .format("jdbc") \
            .option("url", self._zookeeper_url) \
            .option("query", table_query_format) \
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
            .load()

        print("-------------------------------------------hbase维普原始数据---------------------------------------------")
        df.show(truncate=False)

        # 处理学者机构
        relation_author_address_udf = udf(self.relation_author_address, ArrayType(StringType()))
        df_relation_author_address = df.withColumn('relation_author_address', relation_author_address_udf(
                                                                           col('author'), col('org')))
        df_relation_author_address.show(truncate=False)

        # 分割多列
        df_split_author_address = df_relation_author_address.\
            withColumn('author_address', col('relation_author_address').getItem(0)).\
            withColumn('author_order', col('relation_author_address').getItem(1)).\
            withColumn('address_order', col('relation_author_address').getItem(2))

        df_split_author_address.show(truncate=False)

        # 处理关键词
        handle_keyword_udf = udf(self.handle_keyword, StringType())
        df_handle_keyword = df_split_author_address.withColumn('keyword_list', handle_keyword_udf(col('keyword')))

        df_handle_keyword.show(truncate=False)
        print(df_handle_keyword.columns)
        # # 行转列
        # df_explode_author_address = df_format_author_address.withColumn('author_address_rows', explode(
        #                                                                 split(col("format_author_address"), ';;')))

        # df_handle_keyword.coalesce(1).select('ID', 'third_id', 'title', 'author', 'org', 'fund', 'journal', 'year', 'volum', 'issue', 'issn', 'cn', 'page', 'keyword', 'classification_code', 'abstract', 'url', 'school_name', 'school_id', 'updated_time', 'author_address', 'author_order', 'address_order', 'keyword_list').\
        #     write.mode('overwrite').format('csv').option('sep', ',').\
        #     option('encoding', 'utf-8').\
        #     option('header', True).\
        #     save('/tmp/python_spark_project/data/weipu_out.csv')
        df_result = df_handle_keyword.select(
            col('ID'),
            col('third_id').alias('"third_id"'),
            col('title').alias('"title"'),
            col('author').alias('"author"'),
            col('org').alias('"org"'),
            col('fund').alias('"fund"'),
            col('journal').alias('"journal"'),
            col('year').alias('"year"'),
            col('volum').alias('"volum"'),
            col('issue').alias('"issue"'),
            col('issn').alias('"issn"'),
            col('cn').alias('"cn"'),
            col('page').alias('"page"'),
            col('keyword').alias('"keyword"'),
            col('classification_code').alias('"classification_code"'),
            col('abstract').alias('"abstract"'),
            col('url').alias('"url"'),
            col('school_name').alias('"school_name"'),
            col('school_id').alias('"school_id"'),
            col('updated_time').alias('"updated_time"'),
            col('author_address').alias('"author_address"'),
            col('author_order').alias('"author_order"'),
            col('address_order').alias('"address_order"'),
            col('keyword_list').alias('"keyword_list"')
        )


        df_result.\
            write.format('phoenix').mode('append') \
            .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
            .option("table", "SCIENCE.WEIPU_ARTICLE_ANAYLYSIS") \
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
            .save()

        # df_handle_keyword.coalesce(1).write.csv("hdfs:///tmp/python_spark_project/data/output_single.csv", header=True, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('weipu data analysis'). \
        master('local[*]'). \
        getOrCreate()
    weipuDataAnalysis(
        school_name='中国人民大学',
        school_id='88'
    ).obtain_data_analysis_from_hbase()
    spark.catalog.clearCache()
    spark.stop()
