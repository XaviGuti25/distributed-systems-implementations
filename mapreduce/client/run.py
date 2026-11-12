#!/usr/bin/env python3
import sys
import time
import httpx

MASTER_URL = "http://localhost:8000"

<<<<<<< HEAD
"""
2.2.6: The client is responsible for: 

-send a new job to the master
-get back the job_id
-keep asking the master “how is the job going?”
-print progress on the screen until the job finishes
"""


=======
#send request to start jobn to master returns the job id
>>>>>>> de29a68 (viva el celta)
def create_job(data_path: str, map_partitions: int, reduce_partitions: int, app_name: str) -> str:
    # create a new job by sending a request to the master with all the info about the job (app name, data path, number of map and reduce partitions)
    response = httpx.post(
        f"{MASTER_URL}/jobs",
        json={
            "app_name": app_name,
            "data_path": f"./data/{data_path}",
            "map_partitions": map_partitions,
            "reduce_partitions": reduce_partitions,
        },
        timeout=30.0,
    )
    response.raise_for_status()
    job = response.json()
    return job["job_id"]


def get_job_status(job_id: str) -> dict:
    # get the job status by sending a request to the master with the job_id, asks the master “how is the job going?”
    response = httpx.get(f"{MASTER_URL}/jobs/{job_id}", timeout=30.0)
    response.raise_for_status()
    return response.json()

<<<<<<< HEAD

def count_completed(items: list, status: str) -> int: 
    # It takes a list of tasks (mappers or reducers) and a status ("completed") and returns how many tasks have that status.
=======
#how many tasks are completed outof total
def count_completed(items: list, status: str) -> int:
    """Count items with given status"""
>>>>>>> de29a68 (viva el celta)
    return sum(1 for item in items if item["status"] == status)

#read user input create job print job id then loop every 250 ms as for status print proress (count completed) wait repeat until job is compleated
def main():
    # Check command line arguments, we expect 4 arguments: data_folder, map_partitions, reduce_partitions and app_name
    if len(sys.argv) != 5:
        print(
            "Usage: python3 run.py <data_folder> <map_partitions> <reduce_partitions> <app_name>"
        )
        print("Example: python3 run.py medium-text 4 10 word_count")
        sys.exit(1)

    data_folder = sys.argv[1]
    map_partitions = int(sys.argv[2])
    reduce_partitions = int(sys.argv[3])
    app_name = sys.argv[4]

    # createing job
    job_id = create_job(data_folder, map_partitions, reduce_partitions, app_name)
    print(f"job_id: {job_id}")

    # pull job status every 250ms until completion
    last_status = None
    last_map_count = None
    last_reduce_count = None

    while True:
        
        job = get_job_status(job_id)
        current_status = job["status"] # can be "pending", "in-progress" or "completed"
        
        # count how many map tasks are completed
        map_completed = count_completed(job["mappers"], "completed")
        reduce_completed = count_completed(job["reducers"], "completed")

        if (
            # we print the status if the number of completed map/reduce tasks has changed since last time
            current_status != last_status
            or map_completed != last_map_count
            or reduce_completed != last_reduce_count
        ):
            if current_status == "in-progress": # check if we're in mapping or reducing phase
                if map_completed < map_partitions:
                    print(f"in-progress - mapping ({map_completed}/{map_partitions} partitions done)")
                else:
                    print(f"in-progress - reducing ({reduce_completed}/{reduce_partitions} partitions done)")
            elif current_status == "completed":
                print("completed")
                break

            last_status = current_status
            last_map_count = map_completed
            last_reduce_count = reduce_completed

        time.sleep(0.25)


if __name__ == "__main__":
    main()
