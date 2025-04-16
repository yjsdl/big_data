# -*- coding: utf-8 -*-
# @date：2024/10/11 14:31
# @Author：LiuYiJie
# @file： load_data_db_from_file
from pyspark.sql import SparkSession


def obtain_data():
    file_path = r''
    df = spark.read.csv(file_path)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("load data to db").master("local[*]"). \
        config("spark.sql.shuffle.partitions", "100"). \
        config("spark.sql.warehouse.dir", "hdfs://myhdfs/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://hadoop01:9083"). \
        enableHiveSupport(). \
        getOrCreate()