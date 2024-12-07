import argparse
import zmq
import multiprocessing as mp
import json
import time
from queue import Empty

from serialize import *


class PullWorker:
    def __init__(self, num_worker_processors, dispatcher_url):
        self.num_worker_processors = num_worker_processors
        self.dispatcher_url = dispatcher_url
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{self.dispatcher_url}")
        socket.setsockopt(zmq.RCVTIMEO, 10000)

        while True:
            if not self.result_queue.empty():
                task_data = self.result_queue.get()
                socket.send_string(task_data)
                print(f"Worker sent result: {task_data}")
                ack = socket.recv_string()
                print(ack)
                continue

            socket.send_string("START")
            try:
                task = socket.recv_string()

                if task == "NO_TASKS":
                    print("No tasks available")
                    time.sleep(0.01)
                    continue

                # Put the task in the queue
                print(f"Client received task: {task}")
                self.task_queue.put(task)
                time.sleep(0.01)

            except zmq.Again as e:
                print("No task received within timeout")
                time.sleep(5)
                continue

    def process_task(self):
        while True:
            try:
                task = self.task_queue.get(timeout=1)
                print("Processing task...")
                task_data, task_id = task.split("%?%")

                task_data = json.loads(task_data)
                fn_payload = task_data["function_payload"]
                params_payload = task_data["param_payload"]

                fn = deserialize(fn_payload)
                params = deserialize(params_payload)

                args, kwargs = params

                try:
                    result_obj = fn(*args, **kwargs)
                    print(result_obj)
                    task_data["status"] = "COMPLETED"
                    task_data["result"] = serialize(result_obj)
                    print(f"Task processed successfully: {result_obj}")

                except Exception as e:
                    task_data["status"] = "FAILED"
                    task_data["result"] = serialize(e)
                    print(f"Error processing task: {e}")

                self.result_queue.put(json.dumps(task_data) + "%?%" + task_id)

            except Empty:
                time.sleep(0.5)
                continue

    def execute(self):
        # Start the run method in a separate process
        run_process = mp.Process(target=self.run)
        run_process.start()

        # Start worker processes to process tasks from the queue
        processes = []
        for _ in range(self.num_worker_processors):
            p = mp.Process(target=self.process_task)
            p.start()
            processes.append(p)

        run_process.join()
        for p in processes:
            p.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="pull_worker",
        description="Pull Worker for FAAS Project"
    )

    parser.add_argument("num_worker_processors",
                        type=int,
                        help="Number of worker processors for worker")

    parser.add_argument("dispatcher_url",
                        type=str,
                        help="Dispatcher's url to interact with task dispatcher")

    args = parser.parse_args()

    workers = PullWorker(args.num_worker_processors, args.dispatcher_url)
    workers.execute()