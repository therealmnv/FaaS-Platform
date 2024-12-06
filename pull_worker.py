import argparse
import zmq
import multiprocessing as mp
import json
import time

from serialize import *


class PullWorker:
    def __init__(self, num_worker_processors, dispatcher_url):
        self.num_worker_processors = num_worker_processors
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{dispatcher_url}")
        self.socket.setsockopt(zmq.RCVTIMEO, 1000)
        self.message = "START"

    def run(self):
        while True:
            self.socket.send_string(self.message)
            try:
                if self.message != "START":
                    self.message = "START"
                    print(self.socket.recv_string())
                    continue

                task = self.socket.recv_string()

                if task == "NO_TASKS":
                    print("No tasks available")
                    time.sleep(1)
                    continue  # Continue to the next iteration of the while loop

                # Process the task only if it's not "NO_TASKS"
                print(f"Client processing task: {task}")
                task = json.loads(task)
                fn_payload = task["function_payload"]
                params_payload = task["param_payload"]

                fn = deserialize(fn_payload)
                params = deserialize(params_payload)

                args, kwargs = params


                try:
                    result_obj = fn(*args, **kwargs)
                    self.message = serialize(result_obj)
                    print(f"Task processed successfully: {result_obj}")

                except Exception as e:
                    self.message = serialize(e)
                    print(f"Error processing task: {e}")

            except zmq.Again as e:
                print("No task received within timeout")
                time.sleep(1)
                try:
                    self.socket.recv_string()
                except zmq.Again as e:
                    break
                continue

    def execute(self):
        processes = []
        for _ in range(self.num_worker_processors):
            p = mp.Process(target=self.run)
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                        prog="pull_worker",
                        description="Pull Worker for FAAS Project")
    
    parser.add_argument("-d", "--dispatcher_url", required=True,
                        type=str,
                        help="Dispatcher's url to interact with task dispatcher")
    
    parser.add_argument("-w", "--num_worker_processors", required=True,
                        type=int,
                        help="Number of worker processors for worker")

    args = parser.parse_args()

    workers = PullWorker(args.num_worker_processors, args.dispatcher_url)
    workers.execute()