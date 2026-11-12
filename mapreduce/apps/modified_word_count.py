import re

#we emit all values with same key which is wrong
def map(key: str, value: str):
    """
    key: filename
    value: file content
    """
    words = re.findall(r"\b\w+\b", value.lower())

    for word in words:
        if word.startswith("t") and len(word) > 3:
            yield ("count", 1)

#we end up counting in the same reducer all words that sum greater than 3 and start with t instead of doing a per valid word count
def reduce(key: str, values: list[int]):
    return (key, sum(values))


def partitioner(key: str, num_partitions: int):
    return hash(key) % num_partitions

