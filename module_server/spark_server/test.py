# -*- coding: utf-8 -*-
# @date：2024/9/14 11:29
# @Author：LiuYiJie
# @file： test
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def spark_sql_use():
    rdd = sc.textFile("file:///tmp/pycharm_project_211/data/input/word.txt")
    rdd2 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x : [x])

    # 转成DataFrame
    pf = rdd2.toDF(['word'])
    # 注册临时表
    pf.createTempView('words')
    spark.sql("select word, count(*) from words group by word").show()

    # DSL
    pf1 = spark.read.format("text").load("file:///tmp/pycharm_project_211/data/input/word.txt")

    # withColumn方法
    # 对已存在的列的进行操作，如果不修改列名，会替换以前的列的数据，修改列名，会生成一列新数据
    pf1.withColumn("value", F.split("value", " ")).show()
    pf2 = pf1.withColumn("value", F.explode(F.split("value", " ")))
    pf3 = pf2.groupBy("value").count().withColumnRenamed("value", "word").withColumnRenamed("count", "cnt")\
        .orderBy("cnt", ascending=False)
    print(pf3.select(F.avg(pf3['cnt'])).first()["avg(cnt)"])
    pf3.show()
    # pf3.write.mode("overwrite").format('csv').option("header", True).option("sep", "\t").\
    #     save("file:///tmp/pycharm_project_152/data/output/word_result.csv")


if __name__ == '__main__':
    # spark = SparkSession.builder.appName('spark sql test').master('local[*]').getOrCreate()
    # sc = spark.sparkContext
    #
    # spark_sql_use()

    from pyspark.sql import SparkSession
    from pyspark.sql import Row

    # 初始化 SparkSession
    spark = SparkSession.builder.appName("SparkWhereIDExample").getOrCreate()

    # 创建一个示例 DataFrame，包含一些数据行
    data = [
        Row(id='WOS:001059187900006', name='Paper1'),
        Row(id='WOS:001059187900007', name='Paper2'),
        Row(id='WOS:001059187900006', name='Paper3')  # 假设存在重复ID，仅用于演示
    ]
    df = spark.createDataFrame(data)

    # 使用 where 方法并传入条件字符串来筛选数据
    filtered_df = df.where("id = 'WOS:001059187900006'")

    # 展示筛选后的数据
    filtered_df.show()

    spark.stop()
