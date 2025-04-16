# -*- coding: utf-8 -*-
# @date：2024/8/16 12:03
# @Author：LiuYiJie
# @file： etl_test_json

import sys
import json


def transform_to_array_of_maps():
    # 创建一个 array<map<string, string>> 结构
    items = [
        {"subject_id": "123", "category_type": "A"},
        {"subject_id": "456", "category_type": "B"}
    ]

    # 将结果转换为 Hive 可以解析的格式
    return items


# 读取 Hive 传入的数据
for line in sys.stdin:
    line = line.strip()
    # 调用自定义函数进行转换
    result = transform_to_array_of_maps()
    # 输出结果，转换为标准 JSON 格式
    print(json.dumps(result).replace('}, {', '}; {').strip('[]'))
