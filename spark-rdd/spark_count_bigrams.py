from pyspark import SparkContext, SparkConf
import sys
import re

_, source = sys.argv

conf = SparkConf().setAppName("spark-count-bigrams")
sc = SparkContext(conf=conf)

lines = sc.textFile(source)

#remove capitals punnctuation and split into words
def tokenize(s: str):
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    toks = [t for t in s.split() if t]
    return toks
#generate all bygrams from a line
def bigrams_from_line(line: str):
    toks = tokenize(line)
    return [f"{toks[i]} {toks[i+1]}" for i in range(len(toks) - 1)]

#create the list of bigrams per line
bigrams = lines.flatMap(bigrams_from_line)
#create key value pairs and sum out the values after reducing by key
counts = (
    bigrams
    .map(lambda bg: (bg, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortByKey()
)

result = counts.collect()

print(
    """
------------------------------------------
------------------------------------------
------------------------------------------
COUNT BIGRAMS
------------------------------------------
------------------------------------------
------------------------------------------
"""
)

for bg, n in result:
    print(f"{bg} {n}")

sc.stop()
