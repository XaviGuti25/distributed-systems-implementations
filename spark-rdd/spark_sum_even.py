from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
import sys

_, source = sys.argv


conf = SparkConf().setAppName("spark-sum-even")
sc = SparkContext(conf=conf)

numbers_rdd = sc.textFile(source)
#split them lazy
numbers_split_rdd = numbers_rdd.flatMap(lambda line: line.split())
#transform to int lazy
numbers_int_rdd = numbers_split_rdd.map(lambda x: int(x))
even_numbers_rdd = numbers_int_rdd.filter(lambda x: x % 2 == 0)
result = even_numbers_rdd.sum()

print(
    f"""
------------------------------------------
SUM EVEN = {result}
------------------------------------------
"""
)

sc.stop()

