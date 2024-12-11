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
    .config("spark.jars", "/export/server/phoenix/phoenix-client-embedded-hbase-2.5-5.2.0.jar") \
    .getOrCreate()

# 配置 Phoenix 表和 Zookeeper 地址
phoenix_table = "SCIENCE.\"science-article-metadata-v3\""
# zookeeper_url = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181;phoenix.schema.isNamespaceMappingEnabled=true;phoenix.schema.mapSystemTablesToNamespace=true"
zookeeper_url = (
    "jdbc:phoenix:hadoop01:2181:hbase-unsecure;"
    "characterEncoding=UTF-8;"
    "phoenix.schema.isNamespaceMappingEnabled=true;"
)

# 使用 DataFrame 读取数据
df = spark.read \
    .format("jdbc") \
    .option("dbtable", phoenix_table) \
    .option("url", zookeeper_url) \
    .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
    .load()

# 打印数据
df.show()