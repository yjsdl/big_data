# -*- coding: utf-8 -*-
# @date：2025/4/25 17:53
# @Author：LiuYiJie
# @file： kafka_producer_async
import json
import time
from module_server.utils.log import LoggerManager
import module_server.setting as log_setting
from kafka import KafkaProducer
from kafka.errors import KafkaError
from concurrent.futures import ThreadPoolExecutor


class kafkaProducerAsync:
    MAX_RETRIES = 3
    BASE_DELAY = 2  # 秒：指数退避基础

    def __init__(self, topic: str = None):
        self._topic = topic
        self._kafka_producer = KafkaProducer(
            bootstrap_servers=['hadoop01:9092', 'hadoop02:9092', 'hadoop03:9092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',
            linger_ms=50,   # 等待时间，最多等待 50ms 封包发送
            compression_type='gzip',
            retries=0   # 自定义重试逻辑
        )
        self.update_setting()
        self.logger = LoggerManager().get_logger()

    @staticmethod
    def update_setting():
        setattr(log_setting, 'LOG_SPLIT_ERROR', True)

    def on_send_success(self, record_metadata, data):
        msg_id = data.get("id", "unknown_id")
        self.logger.info(
            f"{msg_id} - 成功发送到: "
            f"topic={record_metadata.topic} - partition={record_metadata.partition} - offset={record_metadata.offset}")

    def on_send_error_factory(self, data, topic, retries=0):
        msg_id = data.get("id", "unknown_id")

        def on_send_error(exc):
            self.logger.info(f"{msg_id} - 第 {retries + 1} 次发送失败: {exc}")
            if retries < self.MAX_RETRIES:
                delay = self.BASE_DELAY * (2 ** retries)
                self.logger.info(f"{msg_id} - {delay}s 后重试，第 {retries + 1}/{self.MAX_RETRIES} 次")
                time.sleep(delay)
                future = self._kafka_producer.send(topic, value=data)
                future.add_callback(lambda metadata: self.on_send_success(metadata, data))
                future.add_errback(self.on_send_error_factory(data, topic, retries + 1))
            else:
                self.logger.error(f"{msg_id} - 超过最大重试次数, 请检查")

        return on_send_error

    def send_data(self, data_list: list, max_workers=2):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for data in data_list:
                executor.submit(self._send_async, data, self._topic)

        self._kafka_producer.flush()
        self._kafka_producer.close()

    def _send_async(self, data, topic):
        try:
            future = self._kafka_producer.send(topic, value=data)
            future.add_callback(lambda metadata: self.on_send_success(metadata, data))
            future.add_errback(self.on_send_error_factory(data, topic))
        except KafkaError as e:
            self.logger.error(f'{data.get("id", "")} - 生产失败\n'
                              f'失败原因：{e}')


if __name__ == '__main__':
    _data_list = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ]
    kafkaProducerAsync(topic='testTopic01').send_data(_data_list)
