import argparse
import zmq
import zmq.asyncio
import multiprocessing as mp
from multiprocessing import Queue
import asyncio
import threading

from serialize import *

class PullWorker:
    def __init__(self, num_worker_processors, dispatcher_url):
        self.num_worker_processors = num_worker_processors
        self.free_workers = num_worker_processors

        # Store parameters separately to avoid pickling ZeroMQ context
        self.dispatcher_url = dispatcher_url
        
        # Thread-safe queues for communication between processes
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()
        
        # Synchronization primitives
        self.stop_event = mp.Event()

    @staticmethod
    def worker_process(dispatcher_url, task_queue, result_queue, stop_event):
        """
        Static worker function to be run in separate processes.
        Uses standard ZeroMQ socket for communicating with dispatcher.
        """
        # Create ZeroMQ context inside the process
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{dispatcher_url}")

        while not stop_event.is_set():
            try:
                # Try to get a task with a timeout
                try:
                    task = task_queue.get(timeout=1)
                except Queue.Empty:
                    continue

                if task is None:  # Sentinel value to stop the worker
                    break

                # Ensure task is a dictionary with expected keys
                if not isinstance(task, dict):
                    print(f"Invalid task format: {task}")
                    continue

                fn_payload = task.get("function_payload")
                params_payload = task.get("param_payload")

                if fn_payload is None or params_payload is None:
                    print(f"Missing payload in task: {task}")
                    continue

                fn = deserialize(fn_payload)
                params = deserialize(params_payload)

                try:
                    result_obj = fn(params)
                    result = serialize(result_obj)
                except Exception as e:
                    result = serialize(str(e))

                # Put result in the result queue
                result_queue.put(result)

            except Exception as e:
                print(f"Worker process error: {e}")

        # Clean up ZeroMQ resources
        socket.close()
        context.term()

    async def async_server_communication(self):
        """
        Async method to handle communication with the dispatcher.
        Uses asyncio ZeroMQ for non-blocking communication.
        """
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{self.dispatcher_url}")

        poller = zmq.asyncio.Poller()
        poller.register(socket, zmq.POLLIN)

        try:
            while not self.stop_event.is_set():
                try:
                    # Check if we have free workers and can request a task
                    if self.free_workers > 0:
                        await socket.send_string("START")

                        # Poll with a timeout
                        socks = dict(await poller.poll(10))
                        
                        if socket in socks and socks[socket] == zmq.POLLIN:
                            task_str = await socket.recv_string()

                            if task_str == "NO_TASKS":
                                print("No tasks available")
                                await asyncio.sleep(1)
                                continue
                            
                            # Parse task string into a dictionary
                            try:
                                task = eval(task_str)  # Be cautious with eval
                                if isinstance(task, dict):
                                    self.task_queue.put(task)
                                    self.free_workers -= 1
                                else:
                                    print(f"Invalid task format: {task_str}")
                            except Exception as e:
                                print(f"Error parsing task: {e}")

                            # Check if we have a result to send back
                            if not self.result_queue.empty():
                                result = self.result_queue.get()
                                await socket.send_string(f"RESULT:{result}")
                                self.free_workers += 1

                                # Wait for server acknowledgment
                                await socket.recv_string()
                    
                    await asyncio.sleep(0.5)

                except Exception as e:
                    print(f"Async communication error: {e}")
                    break

        finally:
            socket.close()
            context.term()

    def run(self):
        """
        Main method to start worker processes and async communication.
        """
        # Create and start worker processes
        processes = []
        for _ in range(self.num_worker_processors):
            process = mp.Process(
                target=self.worker_process, 
                args=(
                    self.dispatcher_url, 
                    self.task_queue, 
                    self.result_queue, 
                    self.stop_event
                )
            )
            processes.append(process)
            process.start()

        try:
            # Run async communication
            asyncio.run(self.async_server_communication())
        except KeyboardInterrupt:
            print("Stopping workers...")
        finally:
            # Signal workers to stop
            self.stop_event.set()
            
            # Put sentinel values in task queue to stop workers
            for _ in range(self.num_worker_processors):
                self.task_queue.put(None)
            
            # Wait for all processes to finish
            for process in processes:
                process.join()

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

    # Use spawn method for multiprocessing on Windows
    mp.set_start_method('spawn')

    workers = PullWorker(args.num_worker_processors, args.dispatcher_url)
    workers.run()