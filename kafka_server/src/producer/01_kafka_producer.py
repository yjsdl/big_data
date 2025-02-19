# -*- coding: utf-8 -*-
# @date：2025/2/18 21:24
# @Author：LiuYiJie
# @file： 01_kafka_producer
from kafka import KafkaProducer


def kafka_producer_test():
    # 创建kafkaProducer
    kafkaProducer = KafkaProducer(
        bootstrap_servers=['hadoop01:9092', 'hadoop02:9092', 'hadoop03:9092']
    )

    # 执行数据发送(生产)
    for i in range(0, 20):
        # 同步发送，一条一条发送消息
        record_metadata = kafkaProducer.send(topic='testTopic', value=f'{i}'.encode('utf-8')).get()
        print(record_metadata.topic)  # 消息发送到哪个topic
        print(record_metadata.partition)  # 消息发送到哪个分区
        print(record_metadata.offset)  # 发送消息的偏移量(一个消息就是一个偏移量)

        # 异步发送，先将多条消息暂存在一个池子中，一批一批发送，时间批次为0.1秒
        # kafkaProducer.send(topic='testTopic', value=f'{i}'.encode('utf-8'))

    # 异步发送消息时，休眠0.1秒后也会发送消息
    # time.sleep(0.1)
    # 关闭资源
    kafkaProducer.close()


if __name__ == '__main__':
    kafka_producer_test()


