# -*-coding: Utf-8 -*-
# @File : 02_kafka_producer_async.py
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

    # 异步发送
    future = kafkaProducer.send(topic='testTopic', value=f'123'.encode('utf-8'))

    """
    通过异步的方式，子线程在发送完数据后，不管是成功还是失败，都会通过回调函数，通知给主线程是否成功
    在异步模式下，主线程是不阻塞的，一直不断的往缓存池发送数据
    """

    # 发送成功的回调函数
    def send_success(record_metadata):
        print(record_metadata.topic)  # 消息发送到哪个topic
        print(record_metadata.partition)  # 消息发送到哪个分区
        print(record_metadata.offset)  # 发送消息的偏移量(一个消息就是一个偏移量)

    # 发送失败的回调函数
    def send_error(excp):
        print(f'数据发送失败，失败原因为{excp}')

    future.add_callback(send_success).add_errback(send_error)

    # 关闭资源
    kafkaProducer.close()


if __name__ == '__main__':
    kafka_producer_test()