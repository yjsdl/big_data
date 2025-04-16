# -*-coding: Utf-8 -*-
# @File : 08_窗口函数.py
# author: LiuYiJie
# Time：2024/9/1
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructType, IntegerType, StructField
import pandas as pd
from pyspark.sql import functions as F


# from pyspark.sql.functions import *


def analyze_json_array_value(raw_json_data, field_names):
    try:
        result_list = []
        field_lists = json.loads(field_names)
        print(field_lists)
        json_data = json.loads(raw_json_data)
        column_name = field_lists.pop(0)
        org_lists = json_data.get(column_name, '')

        if not org_lists:
            result_dict = {}
            for column in field_lists:
                result_dict[column] = ''
            return [result_dict]

        for org in org_lists:
            result_dict = {}
            for column in field_lists:
                result_dict[column] = str(org.get(column, ''))
            result_list.append(result_dict)
        return result_list
    except json.JSONDecodeError:
        return []
    except Exception as e:
        print(f"数据处理错误: {e}")
        return []


def array_field_struct_type(field_names):
    field_type = ArrayType(StructType([
        StructField(field, StringType(), True) for field in field_names[1:]]))
    return field_type


def list_to_str(field_names):
    return json.dumps(field_names)


def clean_test():
    pf = spark.sql("select * from science.raw_science_article_metadata limit 5")
    # pf = spark.sql("select * from science_etl_ods.etl_org_article_test limit 10")
    pf.show(truncate=False)

    # df_explode = pf.withColumn("org_dict", F.explode('org_list'))

    # df_explode.show(truncate=False)

    pf.printSchema()
    print(pf.columns)
    # print(pf.first()['org_list'])

    org_names = ['orgs', 'org_id', 'org_name', 'org_number', 'org_index', 'org_all_index', 'first_tag', 'rep_tag',
                 'org_rep_index', 'org_rep_all_index', 'cooperation_tag']
    org_names_str = list_to_str(org_names)

    # 注册一个解析数组的udf函数
    array_struct_type = array_field_struct_type(org_names)

    udf_analyze_array_value = F.udf(analyze_json_array_value, array_struct_type)

    df = pf.select('id', udf_analyze_array_value(pf['json_data'], F.lit(org_names_str).alias('org_names_str')).alias("org_list"))
    df.show(truncate=False)



    # df_explode = df.withColumn("org_dict", F.explode('org_list'))
    #
    # df_explode.show(truncate=False)

    # df.write.mode("append").format('parquet').partitionBy("source_type", "org_school_id").saveAsTable("science_etl_ods.etl_org_article")


if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("create 789"). \
        master("yarn"). \
        config("spark.sql.shuffle.partitions", "100"). \
        config("spark.sql.warehouse.dir", "hdfs://myhdfs/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://hadoop01:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    clean_test()

    # org_names = ['orgs', 'org_id', 'org_name', 'org_number', 'org_index', 'org_all_index', 'first_tag', 'rep_tag',
    #              'org_rep_index', 'org_rep_all_index', 'cooperation_tag']
    # array_struct_type = array_field_struct_type(org_names)
    # print(array_struct_type)
    #
    # # print(pf.first()["id"])
    # # print(pf.select(pf['json_data']).show())
    # # pf.show(truncate=False)
    #
    # return_struct_type = ArrayType(StructType([
    #     StructField('org_id', StringType(), True),
    #     StructField('org_name', StringType(), True),
    #     StructField('org_number', StringType(), True),
    #     StructField('org_index', StringType(), True),
    #     StructField('org_all_index', StringType(), True),
    #     StructField('first_tag', StringType(), True),
    #     StructField('rep_tag', StringType(), True),
    #     StructField('org_rep_index', StringType(), True),
    #     StructField('org_rep_all_index', StringType(), True),
    #     StructField('cooperation_tag', StringType(), True),
    # ]))
    # print(return_struct_type)

