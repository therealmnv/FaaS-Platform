import random
import sys
import json
import time
import argparse
import zmq
import multiprocessing as mp # check for threading

from database import *
from serialize import *

COMPLETE='COMPLETED'
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
        self.timeout = 10

    def run(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')

        print("Server started. Waiting for START requests...")
        while True:
            try:
                message = self.socket.recv_string()
                
                if message == "START":
                    # Check if tasks are available
                    task = pubsub.get_message()
                    if task is None or task['type'] != 'message':
                        # No tasks available
                        print("No tasks available")
                        self.socket.send_string("NO_TASKS")
                    else:
                        task_id = task['data']
                        task_data = redis_client.hget('tasks', task_id) 
                        task_data = json.loads(task_data)
                        task_data["status"] = RUNNING
                        
                        task_data = json.dumps(task_data)
                        redis_client.hset('tasks', task_id, task_data)

                        self.socket.send_string(task_data)
                        
                else:
                    # Receive task result
                    print(f"Server received result: {message}") # TODO: set status
                    self.socket.send_string("RESULT_RECEIVED")
            
            except Exception as e:
                print(f"Server error: {e}")
                break
        
    # def _send_task(self, task_id, task_data):
    #     print(task_data)
    #     self.socket.send(task_data)
    #     task_json = json.loads(task_data)
    #     task_json["status"] = RUNNING
    #     task_data = json.dumps(task_json)    
    #     redis_client.hset('tasks', task_id, task_data)

    #     recv = self.socket.recv_string()
    #     while True:
    #         task = pubsub.get_message()
    #         if task is None or task['type'] != 'message':
    #             continue
    #         task_id = task['data']
    #         task_data = redis_client.hget('tasks', task_id)
    #         if task_data is None:
    #             continue
    #         self._send_task(task_id, task_data)
    #         start_time = time.time()
    #         self._receive_result(task_id, task_data, start_time)

    # def run(self):
    #     pubsub = redis_client.pubsub()
    #     pubsub.subscribe('tasks')
    #     task_id = None
    #     start_time = None

    #     self.socket.recv_string()  # Start loop
    #     while True:
    #         task = pubsub.get_message()
    #         if task is None or task['type'] != 'message':
    #             continue
    #         task_id = task['data']
    #         task_data = redis_client.hget('tasks', task_id)
    #         if task_data is None:
    #             continue
    #         self._send_task(task_id, task_data)
    #         start_time = time.time()
    #         self._receive_result(task_id, task_data, start_time)

    # def _receive_result(self, task_id, task_data, start_time):
    #     result_data = self._receive_string(start_time)

    #     task_json = json.loads(task_data)
        
    #     if result_data is None:
    #         task_json["status"] = FAILED
    #     else:
    #         task_json["status"] = COMPLETE
    #         task_json["result"] = result_data

    #     task_data = json.dumps(task_json)

    #     redis_client.hset('tasks', task_id, task_data)

    # def _receive_string(self, start_time):
    #     while True:
    #         try:
    #             string = self.socket.recv_string(flags=zmq.NOBLOCK)
    #             print("STRING", string)
    #             return string
    #         except zmq.Again:
    #             if start_time and (time.time() - start_time) > self.timeout:
    #                 return None

        
    # def _send_task(self, task_id, task_data):
    #     print(task_data)
    #     self.socket.send(task_data)
    #     task_json = json.loads(task_data)
    #     task_json["status"] = RUNNING
    #     task_data = json.dumps(task_json)    
    #     redis_client.hset('tasks', task_id, task_data)



class PushDispatcher:
    def __init__(self, port):
        self.port = port
        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)
        self.socket.bind(f"tcp://*:{port}")

        self.timeout = 120
        self.active_workers = dict()

    def run(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe('tasks')
        while True:
            worker_id, message = self._receive_worker_message()
            if message == "HEARTBEAT":
                print("New Joiner" , worker_id)
                self._update_workers(worker_id)
            else:
                print("GOT SOMETHING!")
                self._process_result(worker_id, message)
            
            task = pubsub.get_message()
            if task and task['type'] == 'message':
                task_id = task['data']
                task_data = redis_client.hget('tasks', task_id)
                if task_data:
                    self._send_task(task_id, task_data)

    def _receive_worker_message(self):
        worker_id_bytes, message_data = self.socket.recv_multipart()
        worker_id = str(worker_id_bytes)
        if worker_id not in self.active_workers:
            self.active_workers[worker_id] = [time.time(), set()]

        message = message_data.decode()
        return worker_id, message

    def _update_workers(self, worker_id):
        check_time = time.time()
        self.active_workers[worker_id][0] = check_time
        for worker_id, (last_time, task_ids) in list(self.active_workers.items()):
            if (check_time - last_time) > self.timeout:
                del self.active_workers[worker_id]
                for task_id in task_ids:
                    task_data = redis_client.hget("tasks", task_id)
                    task = json.loads(task_data)
                    task["status"] = FAILED
                    task_data = json.dumps(task)
                    redis_client.hset('tasks', task_id, task_data)


    def _process_result(self, worker_id, message):
        task_id, result_data = message.split("%?%")
        task_id = eval(task_id)

        print(task_id)
        print(result_data)

        self.active_workers[worker_id][1].discard(task_id)
        task_data = redis_client.hget('tasks', task_id)
        task = json.loads(task_data)
        if deserialize(result_data) == Exception:
            task["status"] = FAILED
        else:
            task["status"] = COMPLETE
        task['result'] = result_data
        task_data = json.dumps(task)  

        redis_client.hset('tasks', task_id, task_data)


    def _send_task(self, task_id, task_data):
        worker_id = random.choice(tuple(self.active_workers))
        task = json.loads(task_data)
        task["status"] = RUNNING
        task_data = json.dumps(task)
        redis_client.hset('tasks', task_id, task_data)

        message_data = str(task_id) + "%?%" + str(task_data)
        self.socket.send_multipart([eval(worker_id), message_data.encode()])


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
