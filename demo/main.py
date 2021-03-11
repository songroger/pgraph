# -*- coding: utf-8 -*-
# @Time    : 2020/11/26 4:18 下午
# @File    : agg_test.py
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions as sqlfunctions, types
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('rc').getOrCreate()

vertices = spark.createDataFrame(
    [('1', "A"), ('2', "B"), ('3', "C"), ('4', "D"), ('5', "E"), ('6', "F"), ('7', "G"), ('8', "H"),
     ('10', "X")], ["id", "name"])

edges = spark.createDataFrame([('1', '10', 'A0100', 30), ('6', '1', 'A0100', 30), ('2', '1', 'A0100', 40),
                               ('2', '3', 'A0100', 60),
                               ('4', '5', 'A0100', 100), ('5', '10', 'A0100', 49), ('3', '10', 'A0110', 21),
                               ('10', '8', 'A0100', 100),
                               ('8', '7', 'A0100', 100)], ["src", "dst", "edge_type", "ratio"])


def rc_func(id, max_ratio, cnodes): return {"id": id, "max_ratio": max_ratio, 'cnodes': cnodes}


rc_type = types.StructType(
    [types.StructField('id', types.StringType()), types.StructField('max_ratio', types.IntegerType()),
     types.StructField('cnodes', types.ArrayType(types.StringType()))])

rc_func_udf = F.udf(rc_func, rc_type)
vertices = vertices.withColumn("rc", rc_func_udf(vertices["id"], F.lit(0), F.array()))
cached_vertices = AM.getCachedDataFrame(vertices)
g = GraphFrame(cached_vertices, edges)
g.vertices.show()
g.edges.show()


# *****************************************第一次初始化*****************************************
# 初始化入度为0节点的实控人
def agg_src_func(msgs):
    """
    处理接收到src消息后的操作
    :param datas:
    :return:
    """
    max_ratio = -1
    cnodes = []

    for msg in msgs:
        if msg.max_ratio > max_ratio:
            max_ratio = msg.max_ratio
            cnodes = msg.cnodes

    return {'max_ratio': max_ratio, 'cnodes': cnodes}


aggregates = g.aggregateMessages(F.collect_set(AM.msg).alias("agg"),
                                 sendToDst=AM.src["rc"])

agg_src_udf = F.udf(agg_src_func, rc_type)
res = aggregates.withColumn("rc", agg_src_udf("agg")).drop("agg")
print("第一次初始化".center(88, "*"))
init_vertices = g.vertices.join(res, res.id == g.vertices.id, "left").select(g.vertices.id, g.vertices.name, res.rc)

new_vertices = init_vertices.select(init_vertices.id,
                                    init_vertices.name,
                                    F.when(init_vertices.rc.isNull(),
                                           rc_func_udf(init_vertices["id"],
                                                       F.lit(100), F.array(init_vertices['id']))).otherwise(
                                        init_vertices['rc']).alias('rc'))
new_vertices.cache()
new_vertices.show()


# 新增degree_neighbors_info,degree_count 列，存储节点入度节点信息及入度数

def neighbors_info_type_udf_func(id, ratio, rc):
    return {'id': id, 'ratio': ratio, 'rc_nodes': rc}


degree_neighbors_info_schema = types.StructType(
    [types.StructField('id', types.StringType()), types.StructField('ratio', types.IntegerType()),
     types.StructField('rc_nodes', types.ArrayType(rc_type))])

neighbors_info_type_udf = F.udf(neighbors_info_type_udf_func, degree_neighbors_info_schema)

new_vertices = new_vertices.withColumn('degree_neighbors_info', F.lit(0))

init_G = GraphFrame(new_vertices, edges)
init_G.vertices.show()
print("init_G.inDegrees.show".center(88, '*'))
new_vertices.join(init_G.inDegrees, new_vertices.id == init_G.inDegrees.id, "left").select(new_vertices.id,
                                                                                           new_vertices.name,
                                                                                           new_vertices.rc,
                                                                                           F.when(
                                                                                               init_G.inDegrees.inDegree.isNull(),
                                                                                               0).otherwise(
                                                                                               init_G.inDegrees.inDegree).alias(
                                                                                               'indegree_count')).show()


# *****************************************第二次初始化*****************************************
def agg_dst_msg(msgs):
    """
    处理接收到dst消息后的操作
    :param msgs:
    :return:
    """
    pass
