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
        socket.setsockopt(zmq.RCVTIMEO, 1000)
        message = "START"

        while True:
            socket.send_string(message)
            try:
                if message != "START":
                    message = "START"
                    print(socket.recv_string())
                    continue

                task = socket.recv_string()

                if task == "NO_TASKS":
                    print("No tasks available")
                    time.sleep(1)
                    continue  # Continue to the next iteration of the while loop

                # Put the task in the queue
                print(f"Client received task: {task}")
                self.task_queue.put(task)

            except zmq.Again as e:
                print("No task received within timeout")
                time.sleep(1)
                continue

    def process_task(self):
        while True:
            try:
                task = self.task_queue.get(timeout=1)
                task = json.loads(task)
                fn_payload = task["function_payload"]
                params_payload = task["param_payload"]

                fn = deserialize(fn_payload)
                params = deserialize(params_payload)

                args, kwargs = params

                try:
                    result_obj = fn(*args, **kwargs)
                    result = serialize(result_obj)
                    print(f"Task processed successfully: {result_obj}")

                except Exception as e:
                    result = serialize(e)
                    print(f"Error processing task: {e}")

                self.result_queue.put(result)

            except Empty:
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

    parser.add_argument("-d", "--dispatcher_url", required=True,
                        type=str,
                        help="Dispatcher's url to interact with task dispatcher")

    parser.add_argument("-w", "--num_worker_processors", required=True,
                        type=int,
                        help="Number of worker processors for worker")

    args = parser.parse_args()

    workers = PullWorker(args.num_worker_processors, args.dispatcher_url)
    workers.execute()