# -*- coding: utf-8 -*-
# @date：2024/8/16 16:05
# @Author：LiuYiJie
# @file： test


import json
import sys


def transform_to_array_of_maps(input_string):
    try:
        # 去掉字符串中的首尾方括号（如果有）
        input_string = input_string.strip('[]')

        # 通过分号切分字符串
        items = input_string.split(';')

        # 构建 Hive 需要的 array<map<string, string>> 输出格式
        result = []
        for item in items:
            json_data = json.loads(item)
            result.append(json_data)

        # 返回 array<map<string, string>> 格式的结果
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        # 如果处理失败，返回空数组
        return '[]'

# text = ['[{"subject_id": "123", "category_type": "A"};{"subject_id": "456", "category_type": "B"}]']

# 读取 Hive 传入的数据
for line in sys.stdin:
    line = line.strip()
    # 调用自定义函数进行转换
    # result = transform_to_array_of_maps(line)
    # 将结果输出到标准输出
    print("subject_id#123,category_type#A|subject_id#456,category_type#B")