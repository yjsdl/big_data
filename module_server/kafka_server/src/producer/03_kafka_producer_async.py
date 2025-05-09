# -*-coding: Utf-8 -*-
# @File : 02_kafka_producer_async.py
# author: LiuYiJie
# Time：2025/2/24
"""
生产者异步发送消息
"""
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time


class kafkaProducerAsync:
    MAX_RETRIES = 3

    def __init__(self):
        # 创建kafkaProducer
        self._kafka_producer = KafkaProducer(
            bootstrap_servers=['hadoop01:9092', 'hadoop02:9092', 'hadoop03:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=0,  # 自定义重试逻辑
            acks=-1,  # 等待所有副本确认（更可靠）
            linger_ms=50,  # 等待时间，最多等待 50ms 封包发送
            compression_type='gzip'  # 支持压缩（可选：gzip/snappy/lz4/zstd）
        )

    # 发送成功的回调函数
    @staticmethod
    def on_send_success(record_metadata):
        print(record_metadata.topic)  # 消息发送到哪个topic
        print(record_metadata.partition)  # 消息发送到哪个分区
        print(record_metadata.offset)  # 发送消息的偏移量(一个消息就是一个偏移量)

    # 失败回调，带重试
    def on_send_error_factory(self, data, topic, retries=0):
        def on_send_error(excp):
            nonlocal retries
            print(f"❌ 发送失败: {excp}, retry {retries + 1}/{self.MAX_RETRIES}")
            if retries < self.MAX_RETRIES:
                time.sleep(1)  # 可以加个延迟
                retries += 1
                # 再次发送并绑定回调
                future = self._kafka_producer.send(topic, value=data)
                future.add_callback(self.on_send_success).add_errback(self.on_send_error_factory(data, topic, retries))
            else:
                print(f"🚨 重试已达上限，消息发送失败：{data}")
                # 可写入本地文件或告警

        return on_send_error

    def kafka_producer_test(self):

        # 模拟一批数据
        data_list = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
            {"id": 4, "name": "Diana"},
            {"id": 5, "name": "Eve"}
        ]

        # 异步发送，每条仍然是独立消息，Kafka 会内部合批
        """
        通过异步的方式，子线程在发送完数据后，不管是成功还是失败，都会通过回调函数，通知给主线程是否成功
        在异步模式下，主线程是不阻塞的，一直不断的往缓存池发送数据
        """
        for data in data_list:
            future = self._kafka_producer.send(topic='testTopic', value=data)
            future.add_callback(self.on_send_success).add_errback(self.on_send_error_factory(data, 'your_topic'))

        # 关闭资源
        self._kafka_producer.flush()
        self._kafka_producer.close()


if __name__ == '__main__':
    kafkaProducerAsync().kafka_producer_test()
