import argparse
import zmq
import multiprocessing as mp
import json

from serialize import *


class PushWorker:
    def __init__(self, num_worker_processors, dispatcher_url):
        self.num_worker_processors = num_worker_processors
        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)
        self.socket.connect(f"tcp://{dispatcher_url}")


    def run(self):
        with mp.Pool(processes=self.num_worker_processors) as pool:
            while True:
                task_id, task_data = self._receive_task()
                if id:
                    pool.apply_async(self._execute_function, ((task_id, task_data),))
                # self._send_heartbeat()
                


    def _execute_function(self, task_id, task_data):
        task_json = json.loads(task_data)
        fn_payload = task_json["function_payload"]
        params_payload = task_json["param_payload"]

        fn = deserialize(fn_payload)
        params = deserialize(params_payload)

        try:
            result_obj = fn(params)
            result = serialize(result_obj)

        except Exception as e:
            result = serialize(e)

        finally:
            self._send_result(task_id, result)

    def _receive_task(self):
        try:
            id_bytes, data_bytes = self.socket.recv_multipart(flags=zmq.NOBLOCK)
            task_id = id_bytes.decode('utf-8')
            task_data = data_bytes.decode('utf-8')
            return task_id, task_data

        except zmq.Again:
            return None, None

    
    def _send_result(self, task_id, task_result):
        id_bytes = task_id.encode('utf-8')
        result_bytes = task_result.encode('utf-8')
        self.socket.send_multipart([id_bytes, result_bytes])



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                        prog="push_worker",
                        description="Push Worker for FAAS Project")
    
    parser.add_argument("-p", "--port", required=True,
                        type=int,
                        help="Port to interact with task dispatcher")
    
    parser.add_argument("-w", "--num_worker_processors", required=True,
                        type=int,
                        help="Number of worker processors for worker")

    args = parser.parse_args()

    workers = PushWorker(args.num_worker_processors, args.dispatcher_url)
    workers.run()
