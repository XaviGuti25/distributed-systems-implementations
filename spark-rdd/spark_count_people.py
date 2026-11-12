from pyspark import SparkContext, SparkConf
import sys

_, source = sys.argv

conf = SparkConf().setAppName("spark-count-people")
sc = SparkContext(conf=conf)

lines = sc.textFile(source)
#remove spaces and empty lines for correct format
valid = lines.map(lambda x: x.strip()).filter(lambda x: x != "").filter(lambda x: len(x.split()) >= 3)
#create pairs city (key ) 1 reduce by key sum values
city_counts = (
    valid
    .map(lambda x: (x.split()[2], 1))
    .reduceByKey(lambda a, b: a + b)
    .sortByKey()
)

result = city_counts.collect()

print(
    """
------------------------------------------
------------------------------------------
------------------------------------------
COUNT PEOPLE PER CITY
------------------------------------------
------------------------------------------
------------------------------------------
"""
)

for city, n in result:
    print(f"{city} {n}")

sc.stop()
