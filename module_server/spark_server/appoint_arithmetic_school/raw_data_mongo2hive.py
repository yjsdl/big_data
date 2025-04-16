# -*- coding: utf-8 -*-
# @date：2024/10/12 10:43
# @Author：LiuYiJie
# @file： raw_data_mongo2hive
"""
将原始数据从mongo清洗后导入hive
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql.functions import from_json
import pyspark.sql.functions as F
from bson.objectid import ObjectId
import json
import re


def metadata_str_format(raw_metadata):
    metadata = re.sub(r"[^a-zA-Z0-9\u4E00-\u9FA5]", "", raw_metadata)
    metadata = re.sub(r"[\s\t\r\n]", "", metadata)
    metadata = metadata.lower()
    return metadata


def custom_udf_address_list(author_address):
    # 如果地址里面没有作者，而且有多条地址，不做处理
    result_list = []
    if not author_address:
        return None
    if '[' not in author_address:
        if ';' not in author_address:
            result_list.append(author_address)
    else:
        address_list = author_address.split('[')
        # 处理前面一条有作者，后面一条没有的情况,取出作者后，在地址判断
        for name_address in address_list:
            name_address_str = name_address.strip(' ').strip(';')
            if name_address_str != '' and ']' in name_address_str:
                raw_address_list = name_address_str.split(']')
                name = raw_address_list[0]
                raw_address = raw_address_list[1]
                if ';' in raw_address:
                    address = raw_address.split(';')[0]
                    new_name_address = name + ']' + address
                    result_list.append(new_name_address)
                else:
                    result_list.append(name_address_str)
    return result_list or None


def custom_udf_author_list(author_name, name_address_str):
    # 如果没有].表示只有一条地址，作者取全部作者
    result_list = []
    if ']' not in name_address_str:
        if not author_name:
            return None
        name_list = author_name.strip().split(';')
        for raw_name in name_list:
            name = metadata_str_format(raw_name)
            result_list.append(name)
    # 作者取地址里面的作者
    else:
        name_str = name_address_str.strip().split(']')[0]
        name_list = name_str.split(';')
        for raw_name in name_list:
            name = metadata_str_format(raw_name)
            result_list.append(name)
    return result_list or None


def custom_udf_address_format(name_address_str):
    # 如果没有].表示只有一条地址, 取当前地址
    if ']' not in name_address_str:
        address_format = metadata_str_format(name_address_str)
        result = address_format
    else:
        address_str = name_address_str.strip().split(']')[1]
        address_format = metadata_str_format(address_str)
        result = address_format
    return result


def add_id():
    objectID = ObjectId()
    return str(objectID)


def obtain_data_from_mongo():
    pipeline = "[{'$match':{'school_ids': '119'}},{'$project':{'_id':1, 'json_data': 1}}]"
    df = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("spark.mongodb.input.uri",
                # "mongodb://root:kcidea1509!%25)(@101.43.232.153:27017/science2.data_wos_article?authSource=admin") \
                "mongodb://admin:admin@192.168.1.145:27017/science.wos_raw_data?authSource=admin") \
        .option("pipeline", pipeline) \
        .load()

    # 定义json的schema
    schema = StructType([
        StructField("wosAf", StringType(), True),
        StructField("wosC1", StringType(), True),
        StructField("wosEm", StringType(), True),
        StructField("wosAu", StringType(), True),
        StructField("wosSo", StringType(), True),
    ])
    df = df.withColumn('person_str', from_json(df['json_data'], schema))
    raw_article_pf = df.select(df["_id"].alias("third_id"), F.lit('119').alias("school_id"), df["person_str.wosAf"].alias("complete_spell"),
                                       df["person_str.wosC1"].alias("raw_address"), df['person_str.wosEm'].alias('email'),
                                       df["person_str.wosAu"].alias("simple_spell"), df['person_str.wosSo'].alias('journal_name'),
                                       )
    # raw_article_pf.show(n=5, truncate=False)
    raw_article_pf.repartition(5).write.partitionBy("school_id").mode('overwrite').format('hive').saveAsTable('science_arithmetic_test.wos_raw_data')

    # 给每条发文添加id
    unique_id_udf = F.udf(add_id, StringType())
    raw_article_pf = raw_article_pf.withColumn('article_id', unique_id_udf())

    # 处理地址, 拆分地址
    name_address_list_udf = F.udf(custom_udf_address_list, ArrayType(StringType()))

    person_address_list_pf = raw_article_pf.withColumn('name_address_list', name_address_list_udf(raw_article_pf['raw_address']))

    person_address_pf = person_address_list_pf.filter(person_address_list_pf['name_address_list'].isNotNull())

    # 数据量激增
    person_address_pf = person_address_pf.withColumn('name_address_str', F.explode(person_address_pf['name_address_list']))
    # person_address_pf.where("third_id = 'WOS:001059187900006'").show(truncate=False)

    # 后续数据使用当前pf
    person_author_address_pf = person_address_pf.select('article_id', 'third_id', 'school_id', 'complete_spell', 'name_address_str')
    # 缓存存储发文的id，暂时
    # person_author_address_pf.persist()

    # 处理单个地址, 处理作者
    address_format_udf = F.udf(custom_udf_address_format, StringType())
    person_author_address_format_pf = person_author_address_pf.withColumn('article_address_id', unique_id_udf()).withColumn('address_format', address_format_udf(person_author_address_pf['name_address_str']))
    # person_author_address_format_pf.where("third_id = 'WOS:001059187900006'").show(truncate=False)

    # 缓存储存address的id
    # person_author_address_format_pf.persist()
    # person_address_result_pf = person_author_address_format_pf.select(person_author_address_format_pf['article_address_id'].alias('id'), 'article_id', 'third_id', 'school_id', 'address_format')
    # person_address_result_pf.where("third_id = 'WOS:001059187900006'").show(truncate=False)
    # person_address_result_pf.repartition(5).write.partitionBy("school_id").mode('overwrite').format('hive').saveAsTable('science_arithmetic_test.article_address_format')
    # person_author_address_pf.unpersist()

    author_list_udf = F.udf(custom_udf_author_list, ArrayType(StringType()))
    person_author_list_pf = person_author_address_format_pf.withColumn('author_list', author_list_udf(person_author_address_format_pf['complete_spell'], person_author_address_format_pf['name_address_str']))
    person_author_list_notnull_pf = person_author_list_pf.filter(person_author_list_pf['author_list'].isNotNull())
    person_author_pf = person_author_list_notnull_pf.withColumn('author', F.explode(person_author_list_notnull_pf['author_list'])).\
        withColumn('author_id', unique_id_udf())
    person_author_result_pf = person_author_pf.select(person_author_pf['author_id'].alias('id'), 'third_id', 'article_id', 'article_address_id', 'school_id', 'author', 'address_format')
    # person_author_result_pf.where("third_id = 'WOS:001059187900006'").show(truncate=False)

    person_author_result_pf.repartition(5).write.partitionBy('school_id').mode('overwrite').format('hive').saveAsTable('science_arithmetic_test.article_auhtor')

    # person_author_address_format_pf.unpersist()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('raw data mongo to hive').master('yarn'). \
        enableHiveSupport(). \
        config("spark.sql.shuffle.partitions", "100"). \
        config("spark.sql.warehouse.dir", "hdfs://myhdfs/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://hadoop01:9083"). \
        config("hive.exec.dynamic.partition", "true"). \
        config("hive.exec.parallel", "true"). \
        config("hive.exec.dynamic.partition.mode", "nonstrict"). \
        config("hive.exec.compress.intermediate", "true"). \
        config("hive.exec.compress.output", "true"). \
        config("hive.exec.orc.compression.strategy", "COMPRESSION"). \
        getOrCreate()

    obtain_data_from_mongo()

    spark.catalog.clearCache()
    spark.stop()

    # aaddress = 'Shanghai Jiao Tong Univ, Inst Refrigerat & Cryogen, Shanghai 200030, Peoples R China; custom_udf_address;'
    # print(custom_udf_address(aaddress))


