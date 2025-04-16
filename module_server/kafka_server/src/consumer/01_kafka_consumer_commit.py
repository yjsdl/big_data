# -*- coding: utf-8 -*-
# @date：2025/2/19 16:21
# @Author：LiuYiJie
# @file： 01_kafka_consumer_commit
"""
消费者手动提交消息偏移量
"""
from kafka import KafkaConsumer


def kafka_consumer_test():
    # 创建kafkaConsumer
    consumer = KafkaConsumer(
        'testTopic',  # 同时消费多个topic
        bootstrap_servers=['hadoop01:9092', 'hadoop02:9092', 'hadoop03:9092'],
        group_id='group_1',  # 消费组，里面消息偏移量信息
        enable_auto_commit=False,  # 手动提交偏移量
    )

    # 获取消费数据
    for message in consumer:
        topic = message.topic
        partition = message.partition
        offset = message.offset
        key = message.key
        value = message.value.decode('utf-8')
        print(f'从 {topic} 的 {partition} 分区上, 获取到第 {offset} 偏移量的消息为 {key}-{value}')

        # 提交消息偏移量
        consumer.commit()
        consumer.commit_async()


if __name__ == '__main__':
    kafka_consumer_test()
