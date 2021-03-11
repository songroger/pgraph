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

rc_type = types.StructType(
    [types.StructField('id', types.StringType()), types.StructField('max_ratio', types.IntegerType()),
     types.StructField('cnodes', types.ArrayType(types.StringType()))])

rc_func_udf = F.udf(lambda id, max_ratio, cnodes: {"id": id, "max_ratio": max_ratio, 'cnodes': cnodes}, rc_type)
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

# 新增indegree_neighbors_info,degree_count 列，存储节点入度节点信息及入度数

indegree_neighbors_info_schema = types.ArrayType(types.StructType(
    [types.StructField('id', types.StringType()), types.StructField('ratio', types.IntegerType()),
     types.StructField('rc_nodes', types.ArrayType(types.StringType()))]))

neighbors_info_type_udf = F.udf(
    lambda infos: [{'id': v['id'], 'ratio': v['ratio'], 'rc_nodes': v['rc']} for v in infos],
    indegree_neighbors_info_schema)

new_vertices = new_vertices.withColumn('indegree_neighbors_info', F.lit(0))

init_G = GraphFrame(new_vertices, edges)
init_G.vertices.show()
print("init_G.inDegrees.show".center(88, '*'))
new_vertices = new_vertices.join(init_G.inDegrees, new_vertices.id == init_G.inDegrees.id, "left").select(
    new_vertices.id,
    new_vertices.name,
    new_vertices.rc,
    F.when(
        init_G.inDegrees.inDegree.isNull(),
        0).otherwise(
        init_G.inDegrees.inDegree).alias(
        'indegree_count'))

# [[入度数，[节点信息1，节点信息2，节点信息3]]]

new_vertices = new_vertices.select(new_vertices.id, new_vertices.name, new_vertices.indegree_count, new_vertices.rc,
                                   neighbors_info_type_udf(F.array()).alias("indegree_neighbors_info"))

init_G = GraphFrame(new_vertices, edges)

init_G.vertices.show()

# *****************************************迭代操作*****************************************
max_iter = 10


def agg_iter_op(msgs):
    """
    迭代操作
    :param msgs:
    :return:
    """
    # 假设收到的每条消息为[id,edge_ratio,rc]

    # rc: [id,max_ratio,cnodes]
    ret = []
    for msg in msgs:
        if msg.rc.cnodes:
            ret.append({'ratio': msg.edge_ratio, 'id': msg.id, 'cnodes': msg.rc.cnodes})

    return ret


msg_send_udf = F.udf(lambda id, edge_ratio, rc: [id, edge_ratio, rc],
                     types.StructType([types.StructField('id', types.StringType()),
                                       types.StructField('edge_ratio', types.IntegerType()),
                                       types.StructField('rc', rc_type)]))

agg_iter_op_r_type = types.StructType(
    [types.StructField('ratio', types.IntegerType()), types.StructField('id', types.StringType()),
     types.StructField('cnodes', types.ArrayType(types.StringType()))])

agg_iter_op_udf = F.udf(agg_iter_op, types.ArrayType(agg_iter_op_r_type))


def solve_rc(id, neighbors, indegree_count):
    """
    实控人计算逻辑
    :param id:
    :param neighbors:
    :param indegree_count:
    :return:
    """
    if not neighbors or len(neighbors) < indegree_count:
        return {}

    n_ratio_map = dict()
    for neighbor in neighbors:
        for cnode in neighbor.cnodes:
            n_ratio_map.setdefault(cnode, 0)
            n_ratio_map[cnode] += neighbor.ratio

    r_nodes = set()
    r_ratio = 0
    for k, v in n_ratio_map.items():
        if v > r_ratio:
            r_nodes = {k}
            r_ratio = v
        elif v == r_ratio:
            r_nodes.add(k)

    return {'id': id, 'max_ratio': r_ratio, 'cnodes': list(r_nodes)}


solve_rc_udf = F.udf(solve_rc, rc_type)


def rc_result_format(rc, rc1):
    """
    rc,rc1结果转化
    :param rc:
    :param rc1:
    :return:
    """
    return rc if rc.cnodes else rc1


init_G.vertices.show()
for i in range(2):
    print(f"第{i + 1}次迭代".center(88, "*"))
    aggregates = init_G.aggregateMessages(F.collect_set(AM.msg).alias("agg"),
                                          sendToDst=msg_send_udf(AM.src['id'], AM.edge['ratio'], AM.src['rc']))

    res = aggregates.withColumn("indegree_neighbors_info", agg_iter_op_udf("agg")).drop("agg")
    res.show()
    init_vertices = init_G.vertices.join(res, res.id == init_G.vertices.id, "left").select(init_G.vertices.id,
                                                                                           init_G.vertices.name,
                                                                                           res.indegree_neighbors_info,
                                                                                           init_G.vertices.rc,
                                                                                           init_G.vertices.indegree_count)
    print("iter stage 1")
    init_vertices.show()
    init_vertices = init_vertices.withColumn("rc1",
                                             solve_rc_udf(init_vertices["id"], init_vertices["indegree_neighbors_info"],
                                                          init_vertices["indegree_count"]))

    result_format_udf = F.udf(lambda rc, rc1: rc if rc.cnodes else rc1, rc_type)
    init_vertices = init_vertices.withColumn('result',
                                             result_format_udf(init_vertices['rc'], init_vertices['rc1'])).drop(
        'rc').drop('rc1')
    init_vertices = init_vertices.withColumn('rc', init_vertices['result']).drop('result')
    init_G = GraphFrame(init_vertices, edges)
    print("iter stage 2")
    init_vertices.show()
