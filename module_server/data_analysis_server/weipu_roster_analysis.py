# -*- coding: utf-8 -*-
# @date：2024/11/26 11:08
# @Author：LiuYiJie
# @file： weipu_roster_analysis
"""
通过spark读取phoenix映射的hbase表，并对作者地址进行解析，生成标准别名表
"""
import json
import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, BooleanType, ArrayType
from pyspark.sql.functions import explode, split, col, concat_ws, sha2, udf, lit, md5, broadcast, to_timestamp
from datetime import datetime
from pyspark.storagelevel import StorageLevel


def filter_address(broadcast_school_strategy_dict, address_str):
    """
    排除不符合条件的学校
    :param broadcast_school_strategy_dict:
    :param address_str:
    :return: true: 不排除当前地址，false:排除当前地址
    """
    if not address_str:
        return False

    broadcast_school_strategy_dict_value = broadcast_school_strategy_dict.value
    dic_school_list = broadcast_school_strategy_dict_value.get('dic_school_list', [])
    dic_not_school_list = broadcast_school_strategy_dict_value.get('dic_not_school_list', [])

    # 1.地址经过判断策略验证，验证通过=进行下一步，验证不通过=非本校地址
    is_school_flag = False
    for ai_dic_school in dic_school_list:
        condition1 = ai_dic_school['condition1']
        condition2 = ai_dic_school['condition2']
        if condition1 in address_str:
            if condition2 is None:
                is_school_flag = True
            else:
                is_school_flag = condition2 in address_str
        if is_school_flag:
            break

    if is_school_flag:
        # 1.地址经过判断策略，如果通过判断策略验证是本校地址，添加排除策略验证
        # 如果地址包含了排除策略的内容，那就非本校地址
        # 默认地址不通过排除策略验证
        exclude_flag = False
        for ai_dic_not_school in dic_not_school_list:
            condition1 = ai_dic_not_school['condition1']
            condition2 = ai_dic_not_school['condition2']
            if condition1 in address_str:
                if condition2 is None:
                    exclude_flag = True
                else:
                    exclude_flag = condition2 in address_str
            if exclude_flag:
                break
        if exclude_flag:
            is_school_flag = False
    
    return is_school_flag


def analysis_org(broadcast_org_alias_name_dict, address_str):
    """
    通过地址匹配二级机构
    :param broadcast_org_alias_name_dict:
    :param address_str:
    :return:
    """
    org_name_list = broadcast_org_alias_name_dict.value.get('org_name', [])
    alias_name_format_list = broadcast_org_alias_name_dict.value.get('alias_name', [])
    alias_org_name_dict = broadcast_org_alias_name_dict.value.get('split_alias_org_name_dict', {})

    address_slice_str = address_str
    org_list = []

    name_list = org_name_list + alias_name_format_list
    # 让机构名称按照长度排序，避免 《档案学通讯》杂志社 《档案学通讯》杂志社编辑部，这种情况出现
    name_list.sort(key=len, reverse=True)
    while True:
        for org_name in name_list:
            # 如果是以机构开头，说明是这个机构的，替换这个机构后继续查找
            if address_slice_str.startswith(org_name):
                raw_org_name = org_name
                if org_name in alias_name_format_list:
                    org_name = alias_org_name_dict.get(org_name, org_name)
                org_list.append(org_name)
                address_slice_str = address_slice_str.replace(raw_org_name, '', 1)
        address_slice_str = address_slice_str[1:]
        if address_slice_str == '':
            break

    return org_list


def other_org_to_second(broadcast_org_alias_name_dict, org_str):
    other_second_org_dict = broadcast_org_alias_name_dict.value.get('other_second_org_dict', {})
    second_org_name = other_second_org_dict.get(org_str)
    return second_org_name


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
        # phoenix_table = """SCIENCE.RAW_WEIPU_ARTICLE_METADATA"""
        self._org_mysql_db = "science_data_dept"
        self._org_mysql_url = f"jdbc:mysql://43.140.203.187:3306/{self._org_mysql_db}?characterEncoding=UTF-8&connectionCollation=utf8mb4_general_ci&useUnicode=true&useSSL=false&allowMultiQueries=true&serverTimezone=GMT%2b8"

    def ai_dic_not_school_from_mysql(self):
        table_query = f"""
            (
            SELECT ads.condition1, ads.condition2
            FROM ai_dic_school ads
            JOIN ai_dic_not_school adns
            ON ads.school_name = adns.not_school_name
            AND adns.school_id = '{self._school_id}'
            )
            UNION ALL
            (
            SELECT condition1, condition2
            FROM ai_dic_not_school
            WHERE school_id = '{self._school_id}'
            AND (condition1 IS NOT NULL OR condition2 IS NOT NULL )
            )
        """
        df = spark.read.format('jdbc') \
            .option('url', self._mysql_url) \
            .option('query', table_query) \
            .option('user', 'science-read') \
            .option('password', 'readkcidea')\
            .load()
        print('--------------------------------------------学校排除策略------------------------------------------------')
        df.show()
        # 学校排除策略
        dic_not_school_list = df.collect()
        print(dic_not_school_list)
        return dic_not_school_list

    def ai_dic_school_from_mysql(self):
        table_query = f"""
             select school_id, school_name, condition1, condition2 from ai_dic_school 
             where school_id = '{self._school_id}'
        """
        df = spark.read.format('jdbc') \
            .option('url', self._mysql_url) \
            .option('query', table_query) \
            .option('user', 'science-read') \
            .option('password', 'readkcidea') \
            .load()
        print('--------------------------------------------学校判断策略------------------------------------------------')
        df.show()
        # 学校判断策略
        dic_school_lists = df.collect()
        print(dic_school_lists)
        return dic_school_lists

    def org_alias_name_from_mysql(self):
        table_query = f"""
         select short_name, alias_name, second_name from weipu_org_alias_name 
             where school_id = {self._school_id}
        """
        df = spark.read.format('jdbc') \
            .option('url', self._org_mysql_url) \
            .option('query', table_query) \
            .option('user', 'science-data-dept') \
            .option('password', 'datadept1509')\
            .load()
        print("----------------------------------------维普机构表------------------------------------------------------")
        df.show()
        org_alias_name_list = df.collect()
        org_alias_list = [[row['short_name'], row['alias_name']] for row in org_alias_name_list]
        alias_org_dict = {}
        for org_alias in org_alias_list:
            org_name = org_alias[0]
            alias_name = org_alias[1]
            if alias_name != '':
                alias_org_dict[alias_name] = org_name

        other_second_org_list = [[row['short_name'], row['second_name']] for row in org_alias_name_list]
        other_second_org_dict = {}
        for other_second_name in other_second_org_list:
            short_name = other_second_name[0]
            second_name = other_second_name[1]
            other_second_org_dict[short_name] = second_name

        org_name_list = [row['short_name'] for row in org_alias_name_list]
        alias_name_list = [row["alias_name"] for row in org_alias_name_list]

        split_alias_org_name_dict = {}
        alias_name_format_list = []
        for one_alias_name in alias_name_list:
            if one_alias_name != '':
                one_alias_name_list = one_alias_name.split(';;')
                for i in one_alias_name_list:
                    split_alias_org_name_dict[i] = alias_org_dict.get(one_alias_name)
                    alias_name_format_list.append(i)
        result_dict = dict(org_name=org_name_list, alias_name=alias_name_format_list,
                           split_alias_org_name_dict=split_alias_org_name_dict,
                           other_second_org_dict=other_second_org_dict)
        return result_dict

    @staticmethod
    def format_person_address(person_str, address_str):
        def replace_special_char(raw_person_name: str):
            raw_person_name_str = raw_person_name.replace('(', '（').replace(')', '）').replace('\\', '/')
            pattern = r'\d|\（.*?\）'
            person_name_str = re.sub(pattern, '', raw_person_name_str)
            for s in ['（特约专家）', '（指导老师）', '（辅导老师）', '（指导专家）', '指导专家/', '（整理者）', '（评论）', '（摘译）', '（审校）', '（综述）', '（口述）',
                      '（整理）', '等、', '等', '《西安建筑科技大学学报》编辑部', '西安建筑科技大学学报编辑部', '编者', '本刊记者', '指导老师', '、', '（编译', '（摘译',
                      '（设计/撰文）', '（文/图）', '（插图）', '（指导）', '（图/文）', '（点评）', '（编译）', '（译）', '（校）', '（综述', '（图）', '通信作者',
                      '综述']:
                person_name_str = person_name_str.replace(s, '').replace('•', '·')
            person_name_str = person_name_str.strip(*[',，；;。：.·"．']).lower()
            for k in ['课题组', "专家组", "编辑部", "实验室", "测试组", "论坛", "委员会", "专员", "编者", "评论员", '调研组', '工作组', '协作组',
                      '办公室', '合作组', '纪念馆', '教学组', '小组', '研究中心', '项目组', '筹备组', '编写组', '研究组', '评价组', '学组',
                      '分会']:
                if k in person_name_str:
                    person_name_str = ''
            if len(person_name_str) == 1:
                person_name_str = ''
            return person_name_str

        if not address_str or not person_str:
            return ''
        person_list = person_str.split(';')
        person_address_lists = []
        address_list = address_str.split(';')
        address_dict = {}
        for address in address_list:
            address_num = address.split(']')[0].strip(*['。 ['])
            try:
                address_name = address.split(']')[1].strip('')
            except IndexError:
                address_name = ''
            address_dict[address_num] = address_name

        for person in person_list[:1]:
            person_name = person.split('[')[0].strip(*['。 '])
            # 处理一些名字中的脏数据
            person_name = replace_special_char(person_name)
            try:
                person_nums = person.split('[')[1].strip(*['] 。'])
            except IndexError:
                person_nums = ''
            person_num_list = person_nums.split(',') if ',' in person_nums else [person_nums]

            for person_num in person_num_list:
                person_address_name_list = []
                address_name = address_dict.get(person_num, '')
                person_address_name_list.append(person_name)
                person_address_name_list.append(address_name)
                person_address_lists.append(person_address_name_list)

        return person_address_lists

    def obtain_data_analysis_from_hbase(self):
        """
        从hbase中读取数据，并进行作者，地址清洗
        :return:
        """
        # 使用 DataFrame 读取数据
        # TODO
        table_query_format = f"""
        SELECT "third_id", "author", "org", "year" FROM SCIENCE.RAW_WEIPU_ARTICLE_METADATA
        WHERE "school_id" = '{self._school_id}' 
        """
        # table_query = table_query_format.replace(":school_id", str(self._school_id))

        df = spark.read \
            .format("jdbc") \
            .option("url", self._zookeeper_url) \
            .option("query", table_query_format) \
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
            .load()

        print("-------------------------------------------hbase维普原始数据---------------------------------------------")
        # df.show(truncate=False, n=5)

        # 格式化学者机构
        format_person_address_udf = udf(self.format_person_address, ArrayType(ArrayType(StringType())))
        df_format_person_address = df.withColumn("format_person_address", format_person_address_udf(
                                                                           col("author"), col("org")))

        # 行转列
        df_explode_person_address = df_format_person_address.\
            withColumn('person_address_rows', explode(col("format_person_address")))

        # 分割多列
        df_split_person_address = df_explode_person_address.\
            withColumn('name_str', col("person_address_rows").getItem(0)). \
            withColumn('address_str', col('person_address_rows').getItem(1))

        df_result = df_split_person_address.select('third_id', 'name_str', 'address_str', 'year')

        # df_result.show(truncate=False)
        self.init_analysis_data_to_hbase(df_result)

    def init_analysis_data_to_hbase(self, df):
        """
        生成的初始化作者地址表
        :param df:
        :return:
        """
        # 生成唯一键
        # df_row_key = df.withColumn('id', sha2(concat_ws('||', col('name_str'), col('address_str'), col('year')), 256))
        df_row_key = df.withColumn('id', md5(concat_ws('||', df['name_str'], df['address_str'], df['year'])))
        df_result = df_row_key.select(
            col('id').alias('"id"'),
            col('third_id').alias('"third_id"'),
            col('name_str').alias('"name_str"'),
            col('address_str').alias('"address_str"'),
            col('year').alias('"year"')
        )
        #
        print("-------------------------------------生成的初始化作者地址表---------------------------------------------")
        # df_result.show(truncate=False)

        # # 将格式化后的数据缓存，后续做清洗
        # df_result.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        #
        # df_result.write.format("phoenix").mode('append') \
        #     .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
        #     .option("table", 'SCIENCE.WEIPU_AUTHOR_ADDRESS_MEDIATE_TABLE') \
        #     .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
        #     .save()

        self.obtain_school_form_address(df_result)

    def obtain_school_form_address(self, df: DataFrame):
        """
        通过判断策略和排除策略， 过滤学校地址
        :param df:
        :return:
        """
        # TODO
        df_school = df.select('"id"', '"third_id"', '"name_str"', '"address_str"', '"year"')
        dic_school_list = self.ai_dic_school_from_mysql()
        dic_not_school_list = self.ai_dic_not_school_from_mysql()
        school_strategy_dict = dict(dic_school_list=dic_school_list, dic_not_school_list=dic_not_school_list)

        # 广播变量
        broadcast_school_strategy_dict = spark.sparkContext.broadcast(school_strategy_dict)
        filter_school_udf = udf(
            lambda address: filter_address(broadcast_school_strategy_dict, address), BooleanType())

        df_school_filter = df_school.filter(filter_school_udf(col('"address_str"')))

        df_result = df_school_filter.withColumn('"school_id"', lit(self._school_id))
        print("-----------------------------------过滤学校地址，排除非本校地址---------------------------------------")
        # df_result.show(n=5, truncate=False)


        # df_result.write.format('phoenix').mode('append') \
        #     .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
        #     .option("table", "SCIENCE.WEIPU_AUTHOR_ADDRESS_FINAL_TABLE") \
        #     .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
        #     .save()

        # 解析二级机构
        self.analysis_org_from_address(df_result)

    def analysis_org_from_address(self, df):
        org_alias_name_dict = self.org_alias_name_from_mysql()
        print(org_alias_name_dict)
        broadcast_org_alias_name_dict = spark.sparkContext.broadcast(org_alias_name_dict)

        # # 定义 UDF
        # def udf_analysis_org(address):
        #     return self.analysis_org(broadcast_org_alias_name_dict, address)
        #
        # analysis_org_udf = udf(udf_analysis_org, StringType())

        analysis_org_udf = udf(
            lambda address: analysis_org(broadcast_org_alias_name_dict, address), ArrayType(StringType()))
        print("-------------------------解析出最终机构------------------------")
        df_other_second_org_list = df.withColumn('"second_name_list"', analysis_org_udf(col('"address_str"')))
        # df_other_second_org_list.show(truncate=False)

        # 将二级机构行转列
        df_explode_other_second_org = df_other_second_org_list.withColumn('"other_second_org_str"',
                                                                          explode(col('"second_name_list"')))

        df_other_second_org = df_explode_other_second_org.select('"id"', '"third_id"', '"name_str"',
                                                                 '"year"', '"school_id"', '"other_second_org_str"')

        # 重新生成唯一id
        df_other_second_org_id = df_other_second_org.withColumn('"id"', md5(
            concat_ws('||', col('"name_str"'), col('"year"'), col('"school_id"'), col('"other_second_org_str"'))))

        # 过滤姓名和机构为空的
        df_other_second_org_filter = df_other_second_org_id.filter((df_other_second_org['"name_str"'] != '') &
                                                                   (df_other_second_org[
                                                                        '"other_second_org_str"'] != ''))
        # 将其他机构映射到二级机构上
        other_org_to_second_udf = udf(
            lambda org: other_org_to_second(broadcast_org_alias_name_dict, org), StringType())

        df_second_org = df_other_second_org_filter.withColumn('"second_org_str"',
                                                              other_org_to_second_udf(col('"other_second_org_str"')))

        df_second_org_updated_time = df_second_org.withColumn('"updated_time"', to_timestamp(lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))
        # df_second_org_updated_time.show(truncate=False, n=20)

        df_second_org_updated_time.write.format('phoenix').mode('append') \
            .option("zkUrl", "hadoop01,hadoop02,hadoop03:2181") \
            .option("table", "SCIENCE.WEIPU_AUTHOR_ORG_RESULT_TABLE") \
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver") \
            .option("batchsize", 5000)\
            .save()


if __name__ == '__main__':
    # 初始化 SparkSession
    spark = SparkSession.builder \
        .appName("weipu roster analysis") \
        .master("yarn") \
        .getOrCreate()
# .config("spark.jars", "/export/server/spark/jars/phoenix5-spark3-shaded-6.0.0.7.2.17.0-334.jar")
# .config("spark.jars", "/export/server/phoenix/phoenix-client-embedded-hbase-2.5-5.2.0.jar")

    weipuRosterAnalysis(
        school_name='南京农业大学',
        school_id='38'
    ).obtain_data_analysis_from_hbase()
    spark.catalog.clearCache()
    spark.stop()