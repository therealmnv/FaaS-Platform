import argparse
import zmq
import zmq.asyncio
import multiprocessing as mp
from queue import Queue
import time
import asyncio

from serialize import *


class PullWorker:
    def __init__(self, num_worker_processors, dispatcher_url):
        self.num_worker_processors = num_worker_processors
        self.free_workers = num_worker_processors

        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{dispatcher_url}")

        self.poller = zmq.asyncio.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

        self.task_queue = Queue()
        self.result_queue = Queue()
        self.lock = asyncio.Lock()
        self.lock = mp.Lock()


    def worker(self):
        while True:
            task = self.task_queue().get()
            self.t
            if task:
                with self.lock:
                    self.free_workers -= 1
                task = self.task_queue().get()
                fn_payload = task["function_payload"]
                params_payload = task["param_payload"]

                fn = deserialize(fn_payload)
                params = deserialize(params_payload)

                try:
                    result_obj = fn(params)
                    result = serialize(result_obj)

                except Exception as e:
                    result = serialize(str(e))

                finally:
                    self.result_queue.put(result)
                    with self.lock:
                        self.free_workers += 1

    async def async_server_communication(self):
        while True:
            try:
                if self.check_condition():
                    print("HI")
                    await self.socket.send_string("START")

                    # Poll with a 0.01 second timeout (using asyncio)
                    socks = dict(await self.poller.poll(10))
                    
                    if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                        task = await self.socket.recv_string()

                        if task == "NO_TASKS":
                            print("No tasks available")
                            await asyncio.sleep(1)  # Use asyncio.sleep() instead of time.sleep()
                            continue
                        
                        # Process task (simulated work)
                        print(f"Client processing task: {task}")
                        await self.task_queue.put(task)
                        
                        # Send result back to server
                        if not self.result_queue.empty():
                            result = self.result_queue.get()
                            self.socket.send_string(f"RESULT:{result}")
                            self.result_queue.task_done()
                        
                        # Wait for server response to complete REQ-REP cycle
                        await self.socket.recv_string()
                    else:
                        print("No task received within timeout")
                
                # Avoid tight polling loop
                await asyncio.sleep(0.5)
            
            except Exception as e:
                print(f"Client error: {e}")
                break

    def run_workers(self):
        processes = []
        for _ in range(self.num_worker_processors):
            process = mp.Process(target=self.worker)
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

    async def run(self):
        communication = self.async_server_communication()
        self.run_workers()
        await communication

    def check_condition(self):
        # This should be the actual condition for sending "START"
        with self.lock:
            if self.free_workers > 0:
                return True
            else:
                return False
    

    # def run(self):
    #     result = "START" # Perhaps an indicator of 'Worker is on'
    #     with mp.Pool(processes=self.num_worker_processors) as pool:
    #         while True:
    #             task_data = self._request_task(result) # TODO: We must reset the result to "START" after an iteration in the while True? Otherwise it wouldn't request a task after the 1st iteration
    #             task_json = json.loads(task_data)
    #             fn_payload = task_json["function_payload"]
    #             params_payload = task_json["param_payload"]

    #             fn = deserialize(fn_payload)
    #             params = deserialize(params_payload)
    #             try:
    #                 result_obj = pool.apply(fn, (params,)) # TODO: Add a try-except-finally block for handling exceptions
    #                 print(result_obj)
    #                 result = serialize(result_obj)
    #             except Exception as e:
    #                 result = serialize(e)

    # def _request_task(self, task_string):
    #     self.socket.send_string(task_string, encoding='utf-8')
    #     response = self.socket.recv_string(encoding='utf-8')
    #     print("Task Received")
    #     print(response)
    #     return response
    
    # def _execute(self, task_queue, process_id, num_worker_processors):
    #     while True:
    #         self.socket.send_string("START")
    #         task = self.socket.recv_string()
    #         if not task:
    #             self.socket.send_string(" ")
    #             time.sleep(1)
    #             continue
    #         function(task)
            



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
    asyncio.run(workers.run())