# -*- coding: utf-8 -*-
# @date：2024/8/9 10:26
# @Author：LiuYiJie
# @file： hbase_insert_article


import happybase

# con = happybase.Connection(host='192.168.1.211', port=9090)  # 默认9090端口
con = happybase.Connection(host='43.140.203.187', port=9090)  # 默认9090端口
con.open()  # 打开传输
# 查询所有表
print(con.tables())
# # 指定表
# form = con.table('test_db')  # table('test_db')获取某一个表对象
# # 创建表
# families = {
#     "org": dict(max_versions=3)
# }
# table_name = "org_article"
# # games是表名，families是列簇，列簇使用字典的形式表示，每个列簇要添加配置选项，配置选项也要用字典表示
# con.create_table(table_name, families)
#
data = {
    'DATA:名字': '别出大辅助',
    'DATA:等级': '30',
    'DATA:段位': '最强王者',
}

form.put('0001', data)  # 提交数据，0001代表行键，写入的数据要使用字典形式表示
#
# # 下面是查看信息，如果不懂可以继续看下一个
# one_row = form.row('1')  # 获取一行数据,0001是行键
# for value in one_row.keys():  # 遍历字典
#     print(value.decode('utf-8'), one_row[value].decode('utf-8'))  # 可能有中文，使用encode转码

con.close()
