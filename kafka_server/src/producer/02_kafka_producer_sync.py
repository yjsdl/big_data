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
        for i in range(2100, 2200):
            # time.sleep(1)
            value = {"id": i, "name": f"赵六{i}"}
            data = json.dumps(value, ensure_ascii=False)
            record_metadata = kafkaProducer.send(topic='testTopic', value=data.encode('utf-8')).get()
    except KafkaError:
        print('生产数据，出现异常(说明是重试多次后还是发送失败)')
    finally:
        # 关闭资源
        kafkaProducer.close()


if __name__ == '__main__':
    kafka_producer_test()