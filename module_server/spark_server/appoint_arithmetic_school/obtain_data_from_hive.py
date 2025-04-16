# -*- coding: utf-8 -*-
# @date：2024/10/16 19:10
# @Author：LiuYiJie
# @file： obtain_data_from_hive
from pyspark.sql import SparkSession


def obtain_data_hive():
    sql = "select * from science_arithmetic_test.article_auhtor join science_arithmetic_test.wos_raw_data wrd on article_auhtor.third_id = wrd.third_id where author in ('anqinglong', 'baoyuqian', 'baijing', 'baggiolimatteo', 'bernardsrene', 'bianqian', 'ahmedzahoor', 'andewei', 'ahmadnaveed', 'aiqian', 'baiguo', 'baohua', 'baowei', 'baozhiyao', 'abbasjaffar', 'baidingqun', 'bailinquan', 'baixuebing', 'baojiahao', 'baojinsong', 'baoyangyang', 'baozhouzhou', 'ailianzhong', 'andayong', 'anguanghui', 'baiheyuan', 'baowuping', 'bianq', 'abdelrehema', 'ahmednoor', 'andongaolei', 'andresenpeterl', 'bailan', 'baimeirong', 'baiwenkun', 'baiyan', 'baobingkun', 'bianfan', 'bianzeyu', 'abbasisadeq', 'abdullaaynur', 'ahmadkhanzara', 'ahmadmashaal', 'aikemubatuer', 'aipenghui', 'aizhihui', 'anran', 'anyuan', 'ayemm', 'azmatmehmoona');"
    pf = spark.sql(sql)
    pf.printSchema()
    pf.show(truncate=False)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('obtain data from hive').master('local[*]'). \
        enableHiveSupport(). \
        config("spark.sql.shuffle.partitions", "100"). \
        config("spark.sql.warehouse.dir", "hdfs://myhdfs/user/hive/warehouse"). \
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"). \
        config("hive.metastore.uris", "thrift://hadoop01:9083"). \
        config("hive.exec.dynamic.partition", "true"). \
        config("hive.exec.parallel", "true"). \
        config("hive.exec.dynamic.partition.mode", "nonstrict"). \
        config("hive.exec.compress.intermediate", "true"). \
        config("hive.exec.compress.output", "true"). \
        config("hive.exec.orc.compression.strategy", "COMPRESSION"). \
        getOrCreate()

    obtain_data_hive()

    spark.catalog.clearCache()
    spark.stop()
