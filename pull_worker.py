import argparse
import zmq
import multiprocessing as mp

from serialize import *


class PullWorker:
    def __init__(self, num_worker_processors, dispatcher_url):
        self.num_worker_processors = num_worker_processors
        self.address = dispatcher_url
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)

    def run(self):
        result = "START" # Perhaps an indicator of 'Worker is on'
        with mp.Pool(processes=self.num_worker_processors) as pool:
            while True:
                task = self._request_task(result)
                task_object = deserialize(task)
                params = eval(task_object.params)
                result_obj = pool.apply(task_object.fn, (params,))
                result = serialize(result_obj)

    def _request_task(self, task_string):
        self.socket.send_string(task_string, encoding='utf-8')
        response = self.socket.recv_string(encoding='utf-8')
        return response


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                        prog="pull_worker",
                        description="Pull Worker for FAAS Project")
    
    parser.add_argument("-p", "--port", required=True,
                        type=int,
                        help="Port to interact with task dispatcher")
    
    parser.add_argument("-w", "--num_worker_processors", required=True,
                        type=int,
                        help="Number of worker processors for worker")

    args = parser.parse_args()

    workers = PullWorker(args.num_worker_processors, args.dispatcher_url)
    workers.run()
