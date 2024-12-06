import sys
import requests
from serialize import serialize, deserialize
import time


base_url = "http://127.0.0.1:8000/"

valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]

def instant():
    return 1

def perf():
    return 1

def run_function(n_tasks):

    tasks = set()

    resp = requests.post(base_url + "register_function",
                         json={"name": "perf",
                               "payload": serialize(perf)})
    fn_info = resp.json()

    start = time.time()

    for _ in range(n_tasks):
        resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                "payload": serialize(((), {}))})

        task_id = resp.json()["task_id"]
        tasks.add(task_id)
    
    while tasks:
        for task_id in list(tasks):
            resp = requests.get(f"{base_url}result/{task_id}")
            if resp.json()['status'] == "COMPLETED":
                tasks.discard(task_id)

    diff = time.time() - start
    with open("perf/perf.txt", "a") as f:
        f.write(str(diff) + "\n")

    print('DONE!')


if __name__ == "__main__":
    print(sys.argv)
    run_function(int(sys.argv[2]))
