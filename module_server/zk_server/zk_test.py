# -*- coding: utf-8 -*-
# @date：2024/8/23 15:31
# @Author：LiuYiJie
# @file： test

import sys
from kazoo.client import KazooClient, KazooState
import logging

logging.basicConfig(
    level=logging.DEBUG
    , stream=sys.stdout
    , format='query ok---%(asctime)s %(pathname)s %(funcName)s%(lineno)d -- %(levelname)s: -- %(message)s')

# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(
    # hosts='10.0.0.18:2181'
    hosts='43.140.203.187:2181'
    , timeout=10.0  # 连接超时时间
    , logger=logging  # 传一个日志对象进行，方便 输出debug日志

)

# 开始心跳
zk.start()
# 获取子节点
znodes = zk.get_children('/')
print(znodes)

# 开始心跳
zk.start()
# 获取根节点数据和状态
data, stat = zk.get('/')
print('data---', data)
print('stat---', stat)

# 获取根节点的所有子节点，返回的是一个列表，只有子节点的名称
children = zk.get_children("/")
print('所有子节点为', children)

# 下面是根节点的返回值


# 执行stop后所有的临时节点都将失效
zk.stop()
zk.close()
