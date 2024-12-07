# Testing Report

## What we Tested

We tested the following functionalities of our Function as a Service (FaaS) project:

1. **Function Registration**:
    - Valid function registration with serialized payload.
    - Invalid function registration with non-serialized payload.

2. **Function Execution**:
    - Execution of registered functions.
    - Roundtrip of function execution and result retrieval.
    - Handling of exceptions during function execution.
    - Handling of long-running tasks and timeout scenarios.
    - Concurrent execution of multiple tasks.

## Why we Tested

Testing is crucial to ensure the reliability and correctness of our FaaS project. By testing various aspects of the system, we aim to:

1. Verify that functions can be registered and executed correctly.
2. Ensure that the system handles different scenarios, such as invalid inputs, exceptions, and timeouts gracefully.
3. Validate the system's ability to handle concurrent tasks efficiently.

## How we Tested

We used the following approach to test our FaaS project:

1. **Function Registration Tests**:
    - We tested the registration of functions using both valid and invalid payloads (unserialized payload) to ensure that the system correctly handles these scenarios.

    ```
    python
    def test_fn_registration_invalid():
        # Using a non-serialized payload data
        resp = requests.post(base_url + "register_function",
                             json={"name": "hello",
                                   "payload": "payload"})
        assert resp.status_code in [500, 400]

    def test_fn_registration():
        # Using a real serialized function
        serialized_fn = serialize(double)
        resp = requests.post(base_url + "register_function",
                             json={"name": "double",
                                   "payload": serialized_fn})
        assert resp.status_code in [200, 201]
        assert "function_id" in resp.json()
    ```

2. **Function Execution Tests**:
    - We tested the execution of registered functions and verified the status and result of the tasks.

    ```python
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
    ```

3. **Roundtrip Tests**:
    - We tested the complete roundtrip of function execution and result retrieval to ensure that the system correctly processes and returns the results.

    ```python
    def test_roundtrip():
        resp = requests.post(base_url + "register_function",
                             json={"name": "double",
                                   "payload": serialize(double)})
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
            if resp.json()['status'] in ["COMPLETED", "FAILED"]:
                result = deserialize(resp.json()['result'])
                assert result == number * 2
                break
            time.sleep(1)
    ```

4. **Exception Handling Tests**:
    - We tested the system's ability to handle exceptions during function execution and ensure that the task status is updated correctly (in this case "FAILED").

    ```python
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
            if resp.json()['status'] in ["COMPLETED", "FAILED"]:
                assert resp.json()['status'] == "FAILED"
                result = deserialize(resp.json()['result'])
                assert isinstance(result, ValueError)
            time.sleep(1)
    ```

5. **Timeout Handling Tests**:
    - We specifically tested the pull worker's handling of long-running tasks and ensured that tasks are marked as failed if they exceed the timeout (NOTE: at the time of testing the timeout was set to 10s and we purposely gave a sleep time of 15s for function execution, in case the load is high on the machine the test is being run on, we wait monitor the task for 30s).

    ```python
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
            if resp.json()['status'] in ["COMPLETED", "FAILED"]:
                assert resp.json()['status'] == "FAILED"
                break
            time.sleep(1)
    ```

6. **Concurrent Task Execution Tests**:
    - We tested the system's ability to handle multiple concurrent tasks by creating multiple threads and executing tasks simultaneously. The idea behind this test being that when multiple tasks are sent for execution our workers don't mess up the results. 

    ```python
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
                if resp.json()['status'] in ["COMPLETED", "FAILED"]:
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
    ```