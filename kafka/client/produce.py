import argparse
import json
import sys
import hashlib

import httpx

DEFAULT_BROKERS = "1@localhost:8001,2@localhost:8002,3@localhost:8003,4@localhost:8004,5@localhost:8005"


def parse_brokers(brokers_str: str):
    brokers = []
    for item in brokers_str.split(","):
        broker_id, address = item.split("@", 1)
        brokers.append({
            "broker_id": broker_id,
            "address": address,
        })
    return brokers


def get_list_topics(address: str):
    url = f"http://{address}/admin/v1/topics"
    with httpx.Client(timeout=5.0) as client:
        response = client.get(url)
    return response


def post_produce(address: str, topic_partition: str, key: str, payload: str, acks: str):
    url = f"http://{address}/data/v1/produce"

    data = {
        "topic_partition": topic_partition,
        "key": key,
        "payload": payload,
        "acks": acks
    }

    with httpx.Client(timeout=5.0) as client:
        response = client.post(url, json=data)

    return response


def choose_partition(topic: dict, key: str):
    partitions = topic["partitions"]
    partition_count = len(partitions)

    key_hash = int(hashlib.sha256(key.encode()).hexdigest(), 16)
    index = key_hash % partition_count

    return partitions[index]["id"]


def get_topic_from_response(topics_json, topic_name):
    for topic in topics_json.get("topics", []):
        if topic["topic_name"] == topic_name:
            return topic
    return None


def resolve_leader(brokers, response):
    try:
        error_json = response.json()
        detail = error_json.get("detail", "")
        parts = detail.split()
        if len(parts) >= 3:
            leader_broker_id = parts[2].rstrip(",")
            return next(
                (b for b in brokers if b["broker_id"] == leader_broker_id),
                None
            )
    except Exception:
        pass
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic")
    parser.add_argument("-a", "--acks", default="all")
    parser.add_argument("-b", "--brokers", default=DEFAULT_BROKERS)

    args = parser.parse_args()

    brokers = parse_brokers(args.brokers)

    if not brokers:
        print(json.dumps({"detail": "no brokers provided"}, indent=4))
        sys.exit(1)

    # fetch topics
    first_response = get_list_topics(brokers[0]["address"])

    if first_response.status_code == 421:
        leader = resolve_leader(brokers, first_response)
        if leader:
            first_response = get_list_topics(leader["address"])

    if not (200 <= first_response.status_code < 300):
        print(first_response.text)
        sys.exit(1)

    topics_json = first_response.json()
    topic = get_topic_from_response(topics_json, args.topic)

    if topic is None:
        print(json.dumps({"detail": f"topic {args.topic} not found"}, indent=4))
        sys.exit(1)

    # interactive loop
    while True:
        key = input("Key: ").strip()
        if key == "":
            sys.exit(0)

        payload = input("Payload: ").strip()

        topic_partition = choose_partition(topic, key)

        response = post_produce(
            brokers[0]["address"],
            topic_partition,
            key,
            payload,
            args.acks
        )

        if response.status_code == 421:
            leader = resolve_leader(brokers, response)
            if leader:
                response = post_produce(
                    leader["address"],
                    topic_partition,
                    key,
                    payload,
                    args.acks
                )

        if 200 <= response.status_code < 300:
            print("> OK")
        else:
            try:
                print(json.dumps(response.json(), indent=4))
            except Exception:
                print(response.text)
            sys.exit(1)


if __name__ == "__main__":
    main()
