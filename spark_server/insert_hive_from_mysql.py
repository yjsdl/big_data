# -*- coding: utf-8 -*-
# @date：2024/10/8 10:45
# @Author：LiuYiJie
# @file： insert_hive_from_mysql
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F


def obtain_data_from_mysql():
    pf = spark.read.format("jdbc"). \
        option("url",
               "jdbc:mysql://192.168.1.194:3306/science_v9?characterEncoding=UTF-8&connectionCollation=utf8mb4_general_ci&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=GMT%2b8"). \
        option("dbtable", "etl_incites_school_final_org"). \
        option("user", "root"). \
        option("password", "kc1509"). \
        load()
    pf.printSchema()
    pf.show(truncate=False)
    pf.write.mode('overwrite').saveAsTable('science_etl_dws.etl_incites_school_final_org')
    # pf.createOrReplaceTempView('raw_article_data')
    # spark.sql("insert overwrite table science_arithmetic_test.raw_article_data select * from raw_article_data")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("insert data to hive").master('yarn'). \
        config("spark.sql.shuffle.partitions", "100"). \
        config("spark.sql.warehouse.dir", "hdfs://myhdfs/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://hadoop01:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    obtain_data_from_mysql()
    spark.stop()