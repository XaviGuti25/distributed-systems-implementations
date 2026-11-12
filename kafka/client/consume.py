import argparse
import json
import sys
import time

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


def post_consume(address: str, topic_partition: str, last_offset: int, batch_size: int):
    url = f"http://{address}/data/v1/consume"

    payload = {
        "topic_partition": topic_partition,
        "last_offset": last_offset,
        "max_batch_size": batch_size,
        "follower_broker_id": None
    }

    with httpx.Client(timeout=5.0) as client:
        response = client.post(url, json=payload)

    return response


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
    parser.add_argument("topic_partition")
    parser.add_argument("-s", "--batch_size", type=int, default=100)
    parser.add_argument("-b", "--brokers", default=DEFAULT_BROKERS)

    args = parser.parse_args()

    brokers = parse_brokers(args.brokers)

    if not brokers:
        print(json.dumps({"detail": "no brokers provided"}, indent=4))
        sys.exit(1)

    last_offset = -1

    try:
        while True:

            response = post_consume(
                brokers[0]["address"],
                args.topic_partition,
                last_offset,
                args.batch_size
            )

            if response.status_code == 421:
                leader = resolve_leader(brokers, response)
                if leader:
                    response = post_consume(
                        leader["address"],
                        args.topic_partition,
                        last_offset,
                        args.batch_size
                    )

            if not (200 <= response.status_code < 300):
                print(response.text)
                sys.exit(1)

            data = response.json()
            records = data.get("records", [])

            for record in records:
                print(f"[{record['offset']}] [{record['key']}] {record['payload']}")

            if records:
                last_offset = data["last_offset"]

            time.sleep(1)

    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
