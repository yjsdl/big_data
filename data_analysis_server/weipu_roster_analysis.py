# -*- coding: utf-8 -*-
# @date：2024/11/26 11:08
# @Author：LiuYiJie
# @file： weipu_roster_analysis
"""
通过spark读取phoenix映射的hbase表
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, BooleanType
from pyspark.sql.functions import explode, split, col, concat_ws, sha2, udf, lit
from pyspark.sql.functions import broadcast
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel


class weipuRosterAnalysis:
    def __init__(self, school_name: str = None, school_id: str = None):
        self._school_name = school_name
        self._school_id = school_id
        self._zookeeper_url = (
            "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181:hbase-unsecure;"
            "characterEncoding=UTF-8;"
            "phoenix.schema.isNamespaceMappingEnabled=true;"
        )
        self._mysql_db = 'science'
        self._mysql_url = f"jdbc:mysql://43.140.203.187:3306/{self._mysql_db}?characterEncoding=UTF-8&connectionCollation=utf8mb4_general_ci&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=GMT%2b8"
        # phoenix_table = """SCIENCE.SCIENCE_ARTICLE_METADATA"""

    def ai_dic_not_school_from_mysql(self):
        table_query = f"""
             select school_id, school_name, not_school_name, condition1 from ai_dic_not_school 
             where school_name = '{self._school_name}'
        """
        df = spark.read.format('jdbc') \
            .option('url', self._mysql_url) \
            .option('query', table_query) \
            .option('user', 'science-read') \
            .option('password', 'readkcidea')\
            .load()
        df.show()
        # 学校排除策略
        not_school_list = [row['condition1'] for row in df.collect()]
        print(not_school_list)
        return not_school_list

    @staticmethod
    def replace_special_char(person_name: str):
        person_name_str = person_name.replace('(', '（').replace(')', '）').replace('\\', '/')
        for s in ['（特约专家）', '（指导老师）', '（辅导老师）', '（指导专家）', '指导专家/', '（整理者）', '（评论）', '（摘译）', '（审校）', '（综述）', '（口述）',
                  '（整理）', '等、', '等', '《西安建筑科技大学学报》编辑部', '西安建筑科技大学学报编辑部'
                  '（设计/撰文）', '（文/图）', '（插图）', '（指导）', '（图/文）', '（点评）', '（编译）', '（译）', '（校）', '（综述', '（图）', '通信作者']:
            person_name_str = person_name_str.replace(s, '')
        person_name_str.strip(*[',，；;。']).lower()
        return person_name_str

    # 定义 pandas_udf
    @pandas_udf(StringType())
    def format_row_pandas_udf(self, person_col, org_col):
        def process_row(person_str, address_str):
            if not address_str or not person_str:
                return ''
            person_list = person_str.split(';')
            person_address_list = []
            address_list = address_str.split(';')
            address_dict = {}
            for address in address_list:
                address_num = address.split(']')[0].strip(*['。 ['])
                try:
                    address_name = address.split(']')[1].strip('')
                except:
                    address_name = ''
                address_dict[address_num] = address_name

            for person in person_list:
                person_name = person.split('[')[0].strip(*['。 '])
                # 处理一些名字中的脏数据
                person_name = self.replace_special_char(person_name)
                try:
                    person_nums = person.split('[')[1].strip(*['] 。'])
                except:
                    person_nums = ''
                if ',' in person_nums:
                    person_num_list = person_nums.split(',')
                    for person_num in person_num_list:
                        address_name = address_dict.get(person_num, '')
                        person_address_list.append(f"{person_name}::{address_name}")
                else:
                    address_name = address_dict.get(person_nums, '')
                    person_address_list.append(f"{person_name}::{address_name}")

            return ';;'.join(person_address_list)

        # 使用 apply 逐行处理
        return person_col.combine(org_col, process_row)

    def obtain_data_from_hbase(self):
        # 使用 DataFrame 读取数据
        # TODO
        table_query_format = f"""
        SELECT ID, "author", "org", "year" FROM SCIENCE.SCIENCE_ARTICLE_METADATA
        WHERE "school_id" = '{self._school_id}' 
        """
        # table_query = table_query_format.replace(":school_id", str(self._school_id))

        df = spark.read \
            .format("jdbc") \
            .option("url", self._zookeeper_url) \
            .option("query", table_query_format) \
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
            .load()

        df.show(truncate=False)

        # 格式化学者机构
        df_format_person_org = df.withColumn("format_person_org", self.format_row_pandas_udf(df["author"], df["org"]))

        # 行转列
        df_explode_person_org = df_format_person_org.withColumn('person_org_rows', explode(split(col("format_person_org"), ';;')))

        # 分割多列
        df_split_person_org = df_explode_person_org.withColumn('name_str', split("person_org_rows", '::').getItem(0)). \
            withColumn('address_str', split('person_org_rows', '::').getItem(1))

        df_result = df_split_person_org.select(col('ID').alias("third_id"), 'name_str', 'address_str', 'year')
        self.write_data_to_hbase(df_result)

    def write_data_to_hbase(self, df):
        # 生成唯一键
        df_row_key = df.withColumn('id', sha2(concat_ws('||', col('name_str'), col('address_str'), col('year')), 256))
        df_result = df_row_key.select(
            col('id').alias('"id"'),
            col('third_id').alias('"third_id"'),
            col('name_str').alias('"name_str"'),
            col('address_str').alias('"address_str"'),
            col('year').alias('"year"')
        )
        #
        print(df_result.columns)
        df_result.show(truncate=False)

        # # 将格式化后的数据缓存，后续做清洗
        # df_result.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        #
        # df_result.write.format("phoenix").mode('append') \
        #     .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
        #     .option("table", 'SCIENCE.WEIPU_AUTHOR_ORG_MEDIATE_TABLE') \
        #     .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
        #     .save()

        self.obtain_school_form_org(df_result)

    @staticmethod
    def filter_address(broadcast_not_school_list, address_str):
        """
        排除不符合条件的学校
        :param broadcast_not_school_list:
        :param address_str:
        :return:
        """
        for not_name in broadcast_not_school_list.value:
            if not_name in address_str:
                return False
        return True

    def obtain_school_form_org(self, df: DataFrame):
        """
        过滤学校地址，从地址中解析出对应机构
        :param df:
        :return:
        """
        # TODO
        df_school = df.select('"id"', '"third_id"', '"name_str"', '"address_str"', '"year"').filter(col('"address_str"').contains(self._school_name))
        not_school_list = self.ai_dic_not_school_from_mysql()
        if not_school_list:

            # 广播变量
            broadcast_not_school_list = spark.sparkContext.broadcast(not_school_list)
            filter_school_udf = udf(
                lambda address: self.filter_address(broadcast_not_school_list, address), BooleanType())

            df_filter = df_school.filter(filter_school_udf(col("address_str")))
        else:
            df_filter = df_school
        df_result = df_filter.withColumn('"school_id"', lit(self._school_id))
        df_result.show(truncate=False)
        # df_result.write.format('phoenix').mode('append') \
        #     .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
        #     .option("table", "SCIENCE.WEIPU_AUTHOR_ORG_FINAL_TABLE") \
        #     .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
        #     .save()


    def write_school_final_org_to_hbase(self, df):
        # 生成唯一键
        df_row_key = df.withColumn('"id"', sha2(concat_ws('||', col('name_str'), col('address_str'), col('year')), 256))


if __name__ == '__main__':
    # 初始化 SparkSession
    spark = SparkSession.builder \
        .appName("Read Phoenix Table") \
        .master("yarn") \
        .getOrCreate()
# .config("spark.jars", "/export/server/spark/jars/phoenix5-spark3-shaded-6.0.0.7.2.17.0-334.jar")
# .config("spark.jars", "/export/server/phoenix/phoenix-client-embedded-hbase-2.5-5.2.0.jar")

    weipuRosterAnalysis(
        school_name='西安建筑科技大学',
        school_id='168'
    ).obtain_data_from_hbase()
    spark.catalog.clearCache()
    spark.stop()
