# -*- coding: utf-8 -*-
# @date：2024/8/12 11:15
# @Author：LiuYiJie
# @file： phoenix_insert_article
import jaydebeapi
from log_moudle import logger
from queue import Queue


class PhoenixConnectionPool:
    def __init__(self, size, driver_driver=None, jdbc_url=None, driver_jars=None, driver_config=None):
        self.size = size
        self.queue = Queue(maxsize=size)
        for _ in range(size):
            conn = jaydebeapi.connect(driver_driver, jdbc_url, driver_config, driver_jars)
            self.queue.put(conn)

    def get_connection(self):
        return self.queue.get()

    def release_connection(self, conn):
        self.queue.put(conn)

    def close_all(self):
        logger.info('执行结束，关闭所有phoenix连接')
        while not self.queue.empty():
            conn = self.queue.get()
            conn.close()


class phoenixServer:
    def __init__(self):
        # JDBC 驱动路径
        # 注意驱动路径中不能包含中文
        phoenix_jars = [
            r'E:\big_data\software\phoenix\phoenix-client-embedded-hbase-2.5-5.2.0.jar',
            r'E:\big_data\software\phoenix\phoenix-hbase-2.5-5.2.0-bin\lib\log4j-1.2-api-2.18.0.jar',
            r'E:\big_data\software\phoenix\phoenix-hbase-2.5-5.2.0-bin\lib\log4j-api-2.18.0.jar',
            r'E:\big_data\software\phoenix\phoenix-hbase-2.5-5.2.0-bin\lib\log4j-core-2.18.0.jar',
            r'E:\big_data\software\phoenix\phoenix-hbase-2.5-5.2.0-bin\lib\log4j-slf4j-impl-2.18.0.jar',
        ]
        # Phoenix JDBC URL
        jdbc_url = 'jdbc:phoenix:hadoop01:2181:hbase-unsecure;characterEncoding=UTF-8'
        # jdbc_url = 'jdbc:phoenix:43.140.203.187:2181:hbase-unsecure;characterEncoding=UTF-8'
        driver_config = {'phoenix.schema.isNamespaceMappingEnabled': 'true'}
        driver_driver = 'org.apache.phoenix.jdbc.PhoenixDriver'
        # 建立连接
        # 创建连接池
        self.pool = PhoenixConnectionPool(size=2, driver_driver=driver_driver, jdbc_url=jdbc_url,
                                          driver_jars=phoenix_jars, driver_config=driver_config)

        # self.conn = jaydebeapi.connect(driver_class, jdbc_url, driver_config, phoenix_jar)

    def find(self, sql):
        # 创建游标
        data_lists = []
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        try:
            # 执行查询
            cursor.execute(sql)
            # 获取结果
            results = cursor.fetchall()
            for rep in results:
                data_lists.append(rep)
        finally:
            cursor.close()
            self.pool.release_connection(conn)
            logger.info(data_lists)
            return data_lists

    def upsert(self, sql, data: list):
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, data)
            conn.commit()
        except:
            conn.rollback()
            logger.warn('数据插入错误')
        finally:
            self.pool.release_connection(conn)
            cursor.close()


if __name__ == '__main__':
    c = phoenixServer()
    sql = 'select * from SYSTEM.CATALOG'
    result = c.find(sql)
    print(result)
    c.pool.close_all()
