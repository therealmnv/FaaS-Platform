import argparse
import zmq
import multiprocessing as mp
import json

from serialize import *


class PullWorker:
    def __init__(self, num_worker_processors, dispatcher_url):
        self.num_worker_processors = num_worker_processors
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{dispatcher_url}")

    def run(self):
        result = "START" # Perhaps an indicator of 'Worker is on'
        with mp.Pool(processes=self.num_worker_processors) as pool:
            while True:
                task_data = self._request_task(result) # TODO: We must reset the result to "START" after an iteration in the while True? Otherwise it wouldn't request a task after the 1st iteration
                task_json = json.loads(task_data)
                fn_payload = task_json["function_payload"]
                params_payload = task_json["param_payload"]

                fn = deserialize(fn_payload)
                params = deserialize(params_payload)
                try:
                    result_obj = pool.apply(fn, (params,)) # TODO: Add a try-except-finally block for handling exceptions
                    result = serialize(result_obj)
                except Exception as e:
                    result = serialize(e)

    def _request_task(self, task_string):
        self.socket.send_string(task_string, encoding='utf-8')
        response = self.socket.recv_string(encoding='utf-8')
        return response


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
    workers.run()
