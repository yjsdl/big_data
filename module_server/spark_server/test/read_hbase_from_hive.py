# -*- coding: utf-8 -*-
# @date：2024/11/26 11:08
# @Author：LiuYiJie
# @file： read_hbase_from_hive
"""
通过spark读取phoenix映射的hbase表
"""
from pyspark.sql import SparkSession

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("Read Phoenix Table") \
    .master("local[*]") \
    .getOrCreate()

# .config("spark.jars", "/export/server/phoenix/phoenix-client-embedded-hbase-2.5-5.2.0.jar") \

# 配置 Phoenix 表和 Zookeeper 地址
# phoenix_table = """
# SELECT * FROM SCIENCE.SCIENCE_ARTICLE_METADATA
# WHERE "school_id" = '168'
# """
phoenix_table = """SCIENCE.WEIPU_AUTHOR_ORG_MEDIATE_TABLE"""


# zookeeper_url = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181;phoenix.schema.isNamespaceMappingEnabled=true;phoenix.schema.mapSystemTablesToNamespace=true"
zookeeper_url = (
    "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181:hbase-unsecure;"
    "characterEncoding=UTF-8;"
    "phoenix.schema.isNamespaceMappingEnabled=true;"
)

# 使用 DataFrame 读取数据
df = spark.read \
    .format("jdbc") \
    .option("url", zookeeper_url) \
    .option("dbtable", phoenix_table) \
    .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
    .load()

# 打印数据
df.show()
df.printSchema()
print(df.columns)