from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
import sys

_, source = sys.argv

#run a spark app called spark sum
conf = SparkConf().setAppName("spark-sum")
sc = SparkContext(conf=conf)

#read text file lazy
numbers_rdd = sc.textFile(source)
numbers_int_rdd = numbers_rdd.map(lambda x: int(x)) #transform to int also lazy
result = numbers_int_rdd.sum() #now tasks are sent to each worker

print(
    f"""
------------------------------------------
------------------------------------------
------------------------------------------
SUM = {result}
------------------------------------------
------------------------------------------
------------------------------------------
"""
)

sc.stop()
