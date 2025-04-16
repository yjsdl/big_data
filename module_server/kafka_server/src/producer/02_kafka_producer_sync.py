# -*-coding: Utf-8 -*-
# @File : 02_kafka_producer_sync.py
# author: LiuYiJie
# Time：2025/2/24
"""
生产者同步发送消息
"""
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import json


def kafka_producer_test():
    # 创建kafkaProducer
    kafkaProducer = KafkaProducer(
        bootstrap_servers=['hadoop01:9092', 'hadoop02:9092', 'hadoop03:9092'],
        acks=-1,
        linger_ms=1
    )

    # 同步发送
    try:
        for i in range(0, 10):
            # time.sleep(1)
            value = {
                "id": '00001629f36558bfd543b2da1bbbb43c',
                "third_id": "7110639018",
                "title": "音乐喷泉与城市公共建筑空间的融合之美",
                "author": "吴雨婷[1];刘晶[2]",
                "org": "",
                "fund": "",
                "journal": "工业建筑",
                "year": 2023,
                "volum": "53",
                "issue": "7",
                "issn": "1000-8993",
                "cn": "11-2068/TU",
                "page": "-I0039",
                "keyword": "[1017127]音乐喷泉;[9041369]城市公共建筑;[6079899]公共建筑空间;空间设计;城市面貌;设计理念;合理规划;现代科技;",
                "classification_code": "TU986.43",
                "abstract": "城市公共建筑空间是全体居民共有资产,更是城市的重要名片,影响着居民的日常生活,发挥着展示城市面貌、传播地域文化的重要作用。因此,越来越多的人将目光转移到城市公共建筑空间设计中,希望能够通过各种先进的设计理念和技术合理规划城市公共建筑空间,提高其社会价值和生态价值。音乐喷泉是音乐艺术和现代科技结合的具体体现,其将喷涌而出的水柱、五彩的灯光和美妙的音乐结合起来。",
                "url": "http://qikan.cqvip.com/Qikan/Article/Detail?id=7110639018",
                "school_name": '中国人民大学',
                "school_id": '88',
            }
            data = json.dumps(value, ensure_ascii=False)
            record_metadata = kafkaProducer.send(topic='testTopic', value=data.encode('utf-8')).get()
    except KafkaError:
        print('生产数据，出现异常(说明是重试多次后还是发送失败)')
    finally:
        # 关闭资源
        kafkaProducer.close()


if __name__ == '__main__':
    kafka_producer_test()