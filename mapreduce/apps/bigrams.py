# apps/bigrams.py

import re
#extract all pair of words as key with value 1
def map(key: str, value: str):
    words = re.findall(r"[a-zA-Z]+", value.lower())
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i+1]}"
        yield (bigram, 1)
#group by key sum value and eliminate all with count 1
def reduce(key, values):
    total = sum(values)
    if total > 1:
        return key, total

def partitioner(key: str, reduce_partitions: int):
    return hash(key) % reduce_partitions

