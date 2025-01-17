# -*- coding: utf-8 -*-
# @date：2023/5/17 17:31
# @Author：crab-pc
# @file： system_api

import requests
import json
import time
import os


login = "http://admin.science.addup.com.cn/java/science-admin-server/adminUser/login"
school_url = "http://admin.science.addup.com.cn/java/science-admin-server/dic/schoolDic"

dev_login = "http://192.168.1.194:9505/science-admin-server/adminUser/login"
dev_school_url = "http://192.168.1.194:9505/science-admin-server/dic/schoolDic"

user = {
    'username': 'admin',
    'password': '147258369'
}


def get_token():
    payload = f"account={user['username']}&password={user['password']}"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'Pragma': 'no-cache',
        'Referer': 'http://admin.science.addup.com.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    }
    while True:
        response = requests.request("POST", login, headers=headers, data=payload).json()
        if response['success']:
            token = response['data']['accessToken']
            return token
        else:
            time.sleep(2)


def update_school(token):
    headers = {
      'Accept': 'application/json, text/plain, */*',
      'Accept-Language': 'zh-CN,zh;q=0.9',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Content-Length': '0',
      'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
      'Cookie': f'accessToken={token}',
      'Pragma': 'no-cache',
      'Referer': 'http://admin.science.addup.com.cn/',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
      'accessToken': token
    }

    response = requests.request("POST", school_url, headers=headers).json()
    school_sids = response['data']
    school_sid = dict()
    filename = r'Z:\需求文档\代码\science-data-upload\science_data_upload\_base\school_sid.json'
    for one in school_sids:
        school_sid[one['dicName']] = one['id']
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(school_sid, f, ensure_ascii=False)


def main():
    # 首先获取登录token
    token = get_token()
    # 更新学校
    update_school(token)
    return token


if __name__ == '__main__':
    main()

