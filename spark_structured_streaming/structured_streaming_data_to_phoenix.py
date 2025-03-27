# -*- coding: utf-8 -*-
# @date：2025/3/24 16:51
# @Author：LiuYiJie
# @file： structured_streaming_data_to_phoenix
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType


def data_to_phoenix():
    # Kafka 配置
    kafka_bootstrap_servers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    kafka_topic = "testTopic"

    # 读取 Kafka 数据
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 假设 Kafka 数据是 JSON，需要解析成 DataFrame
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
    # schema = "id string, name STRING"

    df_parsed = df_stream.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

    df_result = df_parsed.select(
        'id',
        col('name').alias('"name"')
    )

    # 自定义写入 Phoenix 的函数
    def write_to_phoenix(batch_df, batch_id):
        # batch_df.write \
        #     .format("jdbc") \
        #     .option("url", PHOENIX_JDBC_URL) \
        #     .option("dbtable", "YOUR_PHOENIX_TABLE") \
        #     .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
        #     .mode("append") \
        #     .save()
        batch_df.show(n=100)

        batch_df.write.format('phoenix').mode('append') \
            .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
            .option("table", "SCIENCE.DATA_TO_HBASE_TEST") \
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
            .save()

    # 打印
    # df_result.writeStream.format('console').outputMode('append').start()

    # 使用 foreachBatch 将数据写入 Phoenix
    df_result.writeStream \
        .foreachBatch(write_to_phoenix) \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('structured streaming to phoenix') \
        .getOrCreate()
    data_to_phoenix()
