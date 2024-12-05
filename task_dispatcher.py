import sys
import json
import time
import argparse
import zmq
import multiprocessing as mp # check for threading

from database import *
from serialize import *

COMPLETE='COMPLETE'
RUNNING='RUNNING'
FAILED='FAILED'

redis_client = Redis().get_client()


class LocalDispatcher:
    def __init__(self, num_worker_processors):
        self.num_worker_processors = num_worker_processors
    
    def run(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')

        with mp.Pool(processes=self.num_worker_processors) as pool:
            while True:
                task = pubsub.get_message()
                if task is None or task['type'] != 'message':
                    continue
                task_id = task['data']
                task_data = redis_client.hget('tasks', task_id) 
                if task_data is None:
                    continue

                pool.apply_async(self._execute_task, (task_id, task_data,))


    def _execute_task(self, task_id, task_data):

        task = json.loads(task_data)  
        fn_payload = task["function_payload"]
        params_payload = task["param_payload"]
  
        task["status"] = RUNNING
        task_data = json.dumps(task)
        redis_client.hset('tasks', task_id, task_data)

        fn = deserialize(fn_payload)
        params = deserialize(params_payload)

        # TODO: redirect std out from the function as well to result?

        try:
            result = fn(params)
            result_payload = serialize(result)
            task['status'] = COMPLETE

        except Exception as e:
            result_payload = serialize(e)
            task['status'] = FAILED

        finally:
            task['result'] = result_payload
            task_data = json.dumps(task)
            redis_client.hset('tasks', task_id, task_data)


class PullDispatcher:
    def __init__(self, port):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(f"tcp://127.0.0.1:{port}")
        self.timeout = 120

    def run(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')
        task_id = None
        start_time = None
        while True:
            self._receive_result(task_id, start_time)
            task = pubsub.get_message()
            if task is None or task['type'] != 'message':
                continue
            task_id = task['data']
            task_data = redis_client.hget('tasks', task_id)
            if task_data is None:
                continue
            self._send_task(task_id, task_data)
            start_time = time.time()

    def _receive_result(self, task_id, start_time):
        task_data = self._receive_string(start_time)

        if task_data == "START":
            return
        elif task_data is None:
            task = json.loads(task_data)
            task["status"] = FAILED
            task_data = json.dumps(task)        
        else:
            task = json.loads(task_data)
            task["status"] = COMPLETE
            task_data = json.dumps(task)

        redis_client.hset('tasks', task_id, task_data)

    def _receive_string(self, start_time):
        while True:
            try:
                string = self.socket.recv_string(flags=zmq.NOBLOCK)
                return string
            except zmq.Again:
                if start_time and (time.time() - start_time) > self.timeout:
                    return None

        
    def _send_task(self, task_id, task_data):
        self.socket.send_string(task_data)
        task_json = json.loads(task_data)
        task_json["status"] = RUNNING
        task_data = json.dumps(task_json)    
        redis_client.hset('tasks', task_id, task_data)



class PushDispatcher:
    def __init__(self, port):
        self.port = port
        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)
        self.socket.bind(f"tcp://*:{port}")

    def run(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')
        while True:
            task = pubsub.get_message()
            if task and task['type'] == 'message':
                task_id = task['data']
                task_data = redis_client.hget('tasks', task_id)
                if task_data:
                    self._send_task(task_id, task_data)
            self._receive_result()

    def _send_task(self, task_id, task_data):
        task = json.loads(task_data)
        task["status"] = RUNNING
        task_data = json.dumps(task)

        redis_client.hset('tasks', task_id, task_data)
        data_bytes = task_data.encode('utf-8')
        self.socket.send_multipart([task_id, data_bytes])

    def _receive_result(self):
        try:
            id_bytes, data_bytes = self.socket.recv_multipart(flags=zmq.NOBLOCK)
            task_id = id_bytes.decode('utf-8')
            task_data = data_bytes.decode('utf-8')
            task = json.loads(task_data)

            if deserialize(task['result']) is Exception:
                task["status"] = FAILED    
            else:
                task["status"] = COMPLETE

            task_data = json.dumps(task)

            redis_client.hset('tasks', task_id, task_data)
        except zmq.Again:
            return


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                        prog="task_dispatcher",
                        description="Task Dispatcher for FAAS Project")
    
    parser.add_argument("-m", "--mode", required=True, 
                        choices=['local', 'pull', 'push'],
                        help="Mode to Run Task Dispatcher in. [local/pull/push]", )

    parser.add_argument("-p", "--port", required='local' not in sys.argv,
                        type=int,
                        help="Port to interact with remote worker")
    
    parser.add_argument("-w", "--num_worker_processors", required='local' in sys.argv,
                        type=int,
                        help="Number of worker processors for local worker")
    

    args = parser.parse_args()
    if args.mode == "local":
        dispatcher = LocalDispatcher(args.num_worker_processors)
    else:
        if args.mode == "pull":
            dispatcher = PullDispatcher(args.port)
        else:
            dispatcher = PushDispatcher(args.port)

    dispatcher.run()
