import requests
from .serialize import serialize, deserialize
import logging
import time
import random

base_url = "http://127.0.0.1:8000/"

valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]

def long_running_task(x):
    time.sleep(15)
    return x * 2

def test_pull_worker_timeout_handling():
    resp = requests.post(base_url + "register_function",
                            json={"name": "long_running_task",
                                "payload": serialize(long_running_task)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                "payload": serialize(((number,), {}))})

    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(30):  # Wait longer to ensure timeout
        resp = requests.get(f"{base_url}result/{task_id}")

        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        logging.warning(resp.json())
        if resp.json()['status'] in ["COMPLETED", "FAILED"]:
            logging.warning(f"Task is now in {resp.json()['status']}")
            assert resp.json()['status'] == "FAILED"
            break
        time.sleep(1)