import argparse
import json
import sys

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


def print_response(response):
    try:
        print(json.dumps(response.json(), indent=4))
    except Exception:
        print(response.text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--brokers", default=DEFAULT_BROKERS)
    args = parser.parse_args()

    brokers = parse_brokers(args.brokers)

    if not brokers:
        print(json.dumps({"detail": "no brokers provided"}, indent=4))
        sys.exit(1)

    first_response = get_list_topics(brokers[0]["address"])

    if first_response.status_code == 421:
        leader_broker_id = None
        try:
            error_json = first_response.json()
            detail = error_json.get("detail", "")
            parts = detail.split()
            if len(parts) >= 3:
                leader_broker_id = parts[2].rstrip(",")
        except Exception:
            leader_broker_id = None

        if leader_broker_id is not None:
            leader = next(
                (b for b in brokers if b["broker_id"] == leader_broker_id),
                None
            )
            if leader is not None:
                second_response = get_list_topics(leader["address"])
                print_response(second_response)
                sys.exit(0 if 200 <= second_response.status_code < 300 else 1)

    print_response(first_response)
    sys.exit(0 if 200 <= first_response.status_code < 300 else 1)


if __name__ == "__main__":
    main()