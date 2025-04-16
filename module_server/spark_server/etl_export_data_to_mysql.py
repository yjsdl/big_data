# -*- coding: utf-8 -*-
# @date：2024/9/20 10:14
# @Author：LiuYiJie
# @file： etl_export_data_to_mysql
"""
将数据从hive中导入到mysql
"""
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F


def insert_data_to_db(pf):
    # 写入数据库
    pf.write.mode("overwrite"). \
        format("jdbc"). \
        option("truncate", "true"). \
        option("url",
               "jdbc:mysql://192.168.1.194:3306/science_v9?characterEncoding=UTF-8&connectionCollation=utf8mb4_general_ci&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=GMT%2b8"). \
        option("dbtable", "etl_incites_subject_org_contribution"). \
        option("user", "root"). \
        option("password", "kc1509"). \
        save()

#         option("truncate", "true"). \


def obtain_data_from_etl():
    pf = spark.sql("select * from science_etl_dws.etl_incites_subject_org_contribution")
    pf.printSchema()
    pf.show(truncate=False)
    created_time_pf = pf.withColumn('created_time', F.lit(datetime.now().date()))

    created_time_pf.printSchema()
    insert_data_to_db(created_time_pf)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("export data to db").master('yarn'). \
        config("spark.sql.shuffle.partitions", "100"). \
        config("spark.sql.warehouse.dir", "hdfs://myhdfs/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://hadoop01:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    obtain_data_from_etl()
    spark.stop()
