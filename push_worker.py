import time
import argparse
import zmq
import multiprocessing as mp

mp.set_start_method('fork')
global result_queue
result_queue = mp.Queue()

import json

from serialize import *




def run():
    with mp.Pool(processes=num_worker_processors) as pool:
        while True:
            _send_heartbeat()
            task_id, task_data = _receive_task()
            if task_id:
                pool.apply_async(_execute_function, args=(task_id, task_data))
            if not result_queue.empty():
                res = result_queue.get()
                _send_result(*(result_queue.get()))
            time.sleep(.1)
                
def _send_heartbeat():
    socket.send_string("HEARTBEAT")

def _receive_task():
    try:
        task_message = socket.recv_string(flags=zmq.NOBLOCK)
        task_id, task_data = task_message.split("%?%")
        return task_id, task_data
    except zmq.Again: 
        return None, None
        
def _execute_function(task_id, task_data):
    global result_queue
    task_json = json.loads(task_data)
    fn_payload = task_json["function_payload"]
    params_payload = task_json["param_payload"]

    fn = deserialize(fn_payload)
    params = deserialize(params_payload)

    try:
        result_obj = fn(*params)
        result = serialize(result_obj)

    except Exception as e:
        result = serialize(e)

    finally:
        result_queue.put([task_id, result])

    
def _send_result(task_id, task_result):
    result_message = task_id + "%?%" + task_result
    socket.send_string(result_message)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                        prog="push_worker",
                        description="Push Worker for FAAS Project")
    
    parser.add_argument("-w", "--num_worker_processors", required=True,
                        type=int,
                        help="Number of worker processors for worker")

    parser.add_argument("-d", "--dispatcher_url", required=True,
                        type=str,
                        help="Dispatcher's url to interact with task dispatcher")
    
    args = parser.parse_args()
    
    num_worker_processors = args.num_worker_processors
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect(f"tcp://{args.dispatcher_url}")

    run()
