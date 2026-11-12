from typing import Optional

def map(key: str, value: str):
    words = value.replace(",", " ").replace(".", "").replace(":", "").split()
    result = []
    for word in words:
        yield word.lower(), 1

#word(key) value(1) pairs
def reduce(key: str, values: list) -> str:
    return str(len(values))

#group by key sum values
def partitioner(key: str, partition_count: int) -> str:
    hash_value = 0
    for i, char in enumerate(key):
        hash_value = (hash_value * 16777619 + ord(char)) % partition_count
    return str(hash_value)
