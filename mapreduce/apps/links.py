import re

#extrac quoted fields get last one
def map(key: str, value: str):
    """
    key   -> filename
    value -> file content
    """
    for line in value.splitlines():
        line = line.strip()
        if not line:
            continue

        quoted = re.findall(r'"([^"]*)"', line)

        if len(quoted) >= 3:
            user_agent = quoted[-1] 
            yield (user_agent, 1)

#group by key sum values
def reduce(key: str, values: list[int]):
    return (key, sum(values))


def partitioner(key: str, reduce_partitions: int):
    return hash(key) % reduce_partitions

