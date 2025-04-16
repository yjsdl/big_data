# -*- coding: utf-8 -*-
# @date：2024/8/15 11:15
# @Author：LiuYiJie
# @file： etl_test_to_upper
import sys
import json


for line in sys.stdin:
    line = line.strip()
    id, article_msg, update_time = line.split('\t')
    article_msg_json = json.loads(article_msg)
    article_id = article_msg_json.get('article_id', '')
    article_id_str = article_msg_json.get('article_id_str', '')
    raw_keyword = article_msg_json.get('keyword', '')
    if raw_keyword:
        keyword_list = json.dumps(raw_keyword).replace('[', '').replace(']', '').replace(',', ';')
    else:
        keyword_list = ''

    raw_subjects = article_msg_json.get('subjects', '')

    # if raw_subjects:
    #     subjects_list = json.dumps(raw_subjects).replace('[', '').replace(']', '').replace('}, {', '}; {')
    # else:
    #     subjects_list = ''
    print(str(id) + '\t' + str(article_id) + '\t' + article_id_str + '\t' + keyword_list + '\t')
