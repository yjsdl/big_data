# -*-coding: Utf-8 -*-
# @File : 02_kafka_producer_sync.py
# author: LiuYiJie
# Time：2025/2/24

from kafka import KafkaProducer
from kafka.errors import KafkaError


def kafka_producer_test():
    # 创建kafkaProducer
    kafkaProducer = KafkaProducer(
        bootstrap_servers=['hadoop01:9092', 'hadoop02:9092', 'hadoop03:9092'],
        acks=-1,
        linger_ms=1
    )

    # 同步发送
    try:
        record_metadata = kafkaProducer.send(topic='testTopic', value=f'123'.encode('utf-8')).get()
    except KafkaError:
        print('生产数据，出现异常(说明是重试多次后还是发送失败)')
    finally:
        # 关闭资源
        kafkaProducer.close()


if __name__ == '__main__':
    kafka_producer_test()