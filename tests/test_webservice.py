import requests
from .serialize import serialize, deserialize
import logging
import time
import random
import threading

base_url = "http://127.0.0.1:8000/"

valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]


def test_fn_registration_invalid():
    # Using a non-serialized payload data
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": "payload"})

    assert resp.status_code in [500, 400]


def double(x):
    return x * 2


def test_fn_registration():
    # Using a real serialized function
    serialized_fn = serialize(double)
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialized_fn})

    assert resp.status_code in [200, 201]
    assert "function_id" in resp.json()


def test_execute_fn():
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": serialize(double)})
    fn_info = resp.json()
    assert "function_id" in fn_info

    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((2,), {}))})

    assert resp.status_code == 200 or resp.status_code == 201
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    resp = requests.get(f"{base_url}status/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["task_id"] == task_id
    assert resp.json()["status"] in valid_statuses


def test_roundtrip():
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    print(type(fn_info), fn_info)

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})

    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(20):

        resp = requests.get(f"{base_url}result/{task_id}")

        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        logging.warning(resp.json())
        if resp.json()['status'] in ["COMPLETED", "FAILED"]:
            logging.warning(f"Task is now in {resp.json()['status']}")
            s_result = resp.json()
            logging.warning(s_result)
            result = deserialize(s_result['result'])
            assert result == number*2
            break
        time.sleep(1)

def faulty_task(x):
    raise ValueError("Intentional error")

def test_worker_exception_handling():
    resp = requests.post(base_url + "register_function",
                            json={"name": "faulty_task",
                                "payload": serialize(faulty_task)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                "payload": serialize(((number,), {}))})

    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(20):
        resp = requests.get(f"{base_url}result/{task_id}")

        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        logging.warning(resp.json())
        if resp.json()['status'] in ["COMPLETED", "FAILED"]:
            logging.warning(f"Task is now in {resp.json()['status']}")
            assert resp.json()['status'] == "FAILED"
            s_result = resp.json()
            logging.warning(s_result)
            
            # Log the serialized result before deserialization
            serialized_result = s_result['result']
            logging.warning(f"Serialized result: {serialized_result}")
            
            result = deserialize(serialized_result)
            assert isinstance(result, ValueError)
        time.sleep(1)

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


def test_multiple_concurrent_tasks():
    def execute_task(fn_info, number):
        resp = requests.post(base_url + "execute_function",
                             json={"function_id": fn_info['function_id'],
                                   "payload": serialize(((number,), {}))})

        assert resp.status_code in [200, 201]
        assert "task_id" in resp.json()

        task_id = resp.json()["task_id"]

        for i in range(30):
            resp = requests.get(f"{base_url}result/{task_id}")

            assert resp.status_code == 200
            assert resp.json()["task_id"] == task_id
            logging.warning(resp.json())
            if resp.json()['status'] in ["COMPLETED", "FAILED"]:
                logging.warning(f"Task is now in {resp.json()['status']}")
                assert resp.json()['status'] == "COMPLETED"
                result = deserialize(resp.json()['result'])
                assert result == number * 2
                break
            time.sleep(1)

    # Register the function
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    # Create threads for concurrent execution
    threads = []
    for _ in range(10):  # Number of concurrent tasks
        number = random.randint(0, 10000)
        thread = threading.Thread(target=execute_task, args=(fn_info, number))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()
