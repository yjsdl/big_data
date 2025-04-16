# -*- coding: utf-8 -*-
# @date：2024/10/8 9:04
# @Author：LiuYiJie
# @file： mongo_test
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

# 创建SparkSession
spark = SparkSession \
    .builder \
    .appName("MongoSparkConnectorIntro") \
    .getOrCreate()

pipeline = [
    {
        '$match': {
            'school_name': '东南大学',
        },
    },
    {
        '$lookup': {
            'from': "data_wos_article",
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
            "article_detail.xml1": 0
        },
    },
    {
        '$limit': 5
    }
]

# 连接到MongoDB
df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("spark.mongodb.input.uri",
            "mongodb://science-dev:kcidea1509!%25)(@101.43.232.153:27017/science2.relation_school_wos?authSource=science") \
    .option("pipeline", json.dumps(pipeline)) \
    .load()

# 使用DataFrame进行操作，例如选择所有数据
df.printSchema()
df.show(truncate=False)
spark.stop()
