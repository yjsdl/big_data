# -*- coding: utf-8 -*-
# @date：2024/10/8 9:04
# @Author：LiuYiJie
# @file： mongo_test
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 创建SparkSession
spark = SparkSession \
    .builder \
    .appName("MongoSparkConnectorIntro") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .getOrCreate()


# 连接到MongoDB
df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("spark.mongodb.input.uri",
            "mongodb://admin:admin@192.168.1.145:27017/science.wos_raw_data?authSource=admin") \
    .option("pipeline", "[{'$match':{}},{'$limit':5},{'$project':{'_id':1, 'school_name': 1}}]") \
    .load()

# 使用DataFrame进行操作，例如选择所有数据
df.printSchema()
df.show(truncate=False)
spark.stop()