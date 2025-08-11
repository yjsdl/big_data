# -*- coding: utf-8 -*-
# @date：2025/4/11 10:35
# @Author：LiuYiJie
# @file： weipu_data_structured_streaming
"""
从kafka中读取数据，经过处理存储到phoenix
"""
import re
import json
import bson
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, IntegerType, StructField, StructType
from pyspark.sql.functions import md5, udf, col, split, explode, from_json


class weipuDataAnalysis:
    def __init__(self):
        self._zookeeper_url = (
            "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181:hbase-unsecure;"
            "characterEncoding=UTF-8;"
            "phoenix.schema.isNamespaceMappingEnabled=true;"
        )
        # Kafka 配置
        self.kafka_bootstrap_servers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
        self.kafka_topic = "testTopic"
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
            address_id_dict[address_name] = str(bson.ObjectId())
            address_order_list.append({'id': address_id_dict.get(address_name, ''), 'authorName': address_name,
                                       'number': address_order, 'repNumber': '', 'repTag': '', 'sourceType': 3})

        for author in author_list:
            author_name = author.split('[')[0].strip(*['。 '])
            author_id_dict[author_name] = str(bson.ObjectId())

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
            keyword_id_list.append({'id': str(bson.ObjectId()), 'name': keyword, 'remark': ''})

        return json.dumps(keyword_id_list, ensure_ascii=False)

    def article_data_analysis(self, batch_df, batch_id):
        """
        从kafka中读取数据，并进行作者，地址清洗
        :return:
        """
        try:
            print("-------------------------------------------hbase维普原始数据-------------------------------------------")

            # 处理学者机构
            relation_author_address_udf = udf(self.relation_author_address, ArrayType(StringType()))
            df_relation_author_address = batch_df.withColumn('relation_author_address', relation_author_address_udf(
                                                                               col('author'), col('org')))

            # 分割多列
            df_split_author_address = df_relation_author_address.\
                withColumn('author_address', col('relation_author_address').getItem(0)).\
                withColumn('author_order', col('relation_author_address').getItem(1)).\
                withColumn('address_order', col('relation_author_address').getItem(2))

            # 处理关键词
            handle_keyword_udf = udf(self.handle_keyword, StringType())
            df_handle_keyword = df_split_author_address.withColumn('keyword_list', handle_keyword_udf(col('keyword')))

            df_handle_keyword.show(truncate=False)

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

            df_result.write.format('phoenix').mode('append') \
                .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
                .option("table", "SCIENCE.WEIPU_ARTICLE_ANALYSIS") \
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
                .save()
        except Exception as e:
            print(f"[Batch {batch_id}] 错误: {e}")

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
            StructField("third_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("org", StringType(), True),
            StructField("fund", StringType(), True),
            StructField("journal", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("volum", StringType(), True),
            StructField("issue", StringType(), True),
            StructField("issn", StringType(), True),
            StructField("cn", StringType(), True),
            StructField("page", StringType(), True),
            StructField("keyword", StringType(), True),
            StructField("classification_code", StringType(), True),
            StructField("abstract", StringType(), True),
            StructField("url", StringType(), True),
            StructField("school_name", StringType(), True),
            StructField("school_id", StringType(), True)
        ])
        # schema = "id string, name STRING"

        df_cast_value = df_stream.selectExpr("CAST(value AS STRING)", 'timestamp') \
            .select(from_json(col("value"), schema).alias("data"), col('timestamp').alias('updated_time')) \
            .select("data.*", 'updated_time')

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
        appName('weipu data analysis'). \
        master('yarn'). \
        getOrCreate()
    weipuDataAnalysis().read_article_from_kafka()
    spark.catalog.clearCache()
    spark.stop()