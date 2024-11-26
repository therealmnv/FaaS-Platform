import sys
import json
import argparse
import dill
import codecs
import zmq
import multiprocessing as mp

from database import *

COMPLETE='COMPLETE'
RUNNING='RUNNING'
FAILED='FAILED'

redis_client = Redis().get_client()


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))

class LocalDispatcher:
    def __init__(self, num_worker_processors):
        self.num_worker_processors = num_worker_processors
    
    def run(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')

        with mp.Pool(processes=self.num_worker_processors) as pool:
            while True:
                task_id = pubsub.get_message()
                task_data = redis_client.hget('tasks', task_id)      
                task = json.loads(task_data)  

                fn_payload = task["function_payload"]
                param_payload = task["param_payload"]
                pool.apply_async(self._execute_task, (task_id, fn_payload, param_payload,))


    def _execute_task(self, task_id, fn_payload, param_payload):

        task_data = redis.hget('tasks', task_id)
        task = json.loads(task_data)    

        task["status"] = RUNNING
        task_data = json.dumps(task)
        redis_client.hset('tasks', task_id, task_data)

        fn = deserialize(fn_payload)
        param = deserialize(param_payload)

        result = fn(*param)
        result_payload = serialize(result)

        task['status'] = COMPLETE
        task['result'] = result_payload
        task_data = json.dumps(task)
        redis_client.hset('tasks', task_id, task_data)


class PullDispatcher:
    def __init__(self, port):
        self.port = port
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)

    def run(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')
        while True:
            self._receive_result()
            task_id = pubsub.get_message()
            if task_id:
                task_data = redis_client.hget('tasks', task_id)
                self._send_task(task_data)

    def _receive_result(self, task_id):
        task_data = self.socket.recv_string()
        if task_data == "START":
            return
        
        task = json.loads(task_data)
        task["status"] = COMPLETE
        task_data = json.dumps(task)

        redis_client.hset('tasks', task_id, task_data)
        
    def _send_task(self, task_data):
        self.socket.send_string(task_data)


class PushDispatcher:
    def __init__(self, port):
        self.port = port
        context = zmq.Context()
        self.socket = context.socket(zmq.DEALER)

    def run(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')
        while True:
            task_id = pubsub.get_message()
            if task_id:
                task_data = redis_client.hget('tasks', task_id)
                self._send_task(task_data)
            self._receive_result(task_data)

    def _send_task(self, task_id, task_data):
        task = json.loads(task_data)
        task["status"] = RUNNING
        task_data = json.dumps(task)

        redis_client.hset('tasks', task_id, task_data)
        data_bytes = task_data.encode('utf-8')
        id_bytes = task_id.encode('utf-8')
        self.socket.send_multipart([id_bytes, data_bytes])

    def _receive_result(self):
        try:
            id_bytes, data_bytes = self.socket.recv_multipart(flags=zmq.NOBLOCK)
            task_id = id_bytes.decode('utf-8')
            task_data = data_bytes.decode('utf-8')

            task = json.loads(task_data)
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






        

    
