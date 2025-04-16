# -*- coding: utf-8 -*-
# @date：2024/10/8 10:45
# @Author：LiuYiJie
# @file： insert_hive_from_mysql
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F


# # 自定义分区器
# class CustomPartitioner(pyspark.Partitioner):
#     def numPartitions(self):
#         # 返回分区数
#         return 10
#
#     def getPartition(self, key):
#         # 根据键值对返回分区号
#         return hash(key) % self.numPartitions()

def obtain_data_from_mysql():
    jdbcUrl = "jdbc:mysql://192.168.1.194:3306/science_v10?characterEncoding=UTF-8&connectionCollation=utf8mb4_general_ci&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=GMT%2b8"
    article_count = 860461
    numPartitions = 20
    stride = article_count // numPartitions
    predicates = []
    for i in range(numPartitions):
        offset = i * stride
        if i == numPartitions - 1:
            predicates.append(f"1 = 1 LIMIT {article_count - offset} OFFSET {offset}")
        else:
            predicates.append(f"1 = 1 LIMIT {stride} OFFSET {offset}")
    print(len(predicates), predicates)
    properties = {}
    properties.setdefault("user", "root")
    properties.setdefault("password", "kc1509")

    pf = spark.read.jdbc(jdbcUrl, "(select * from article_author_address limit 860461) as tmp", predicates=predicates, properties=properties)

    pf.printSchema()
    # pf.show(truncate=False)
    print(pf.rdd.getNumPartitions())

    pf.write.saveAsTable('science_arithmetic_test.raw_article_data', format='hive', mode='overwrite')
    # pf.repartition(5).createOrReplaceTempView('raw_article_data')
    # spark.sql("insert overwrite table science_arithmetic_test.raw_article_data select * from raw_article_data")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("insert data to hive").master('yarn'). \
        config("spark.sql.shuffle.partitions", "50"). \
        config("spark.sql.warehouse.dir", "hdfs://myhdfs/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://myhdfs:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    obtain_data_from_mysql()
    spark.stop()