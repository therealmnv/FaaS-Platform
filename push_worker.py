import sys
import threading
import zmq

import dill
import codecs


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


class PushWorkers:
    def __init__(self, n_workers, dispatcher_url):
        self.n_workers = n_workers
        self.address = dispatcher_url
        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)

    def run(self):
        self.threads = [threading.Thread(target=self._run_worker, name = idx) 
                        for idx in range(self.n_workers)]
        for thread in self._threads:
            thread.start()

    def _run_worker(self):
        while True:
            id, task = self._receive_task()
            task_object = deserialize(task)
            params = eval(task_object.params)
            result_obj = task_object.fn(*params)
            result = serialize(result_obj)
            self._send_result(id, result)

    def _receive_task(self):
        id, task = self.socket.recv_multipart()
        task_str = task.decode('utf-8')
        return id, task_str
    
    def _send_result(self, id, result_str):
        result = result_str.encode('utf-8')
        self.socket.send_multipart([id, result])



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Please provide fields: <num_worker_processors> <dispatcher url>")
        sys.exit(1)

    n_workers = int(sys.argv[1])
    dispatcher_url = sys.argv[2]

    workers = PushWorkers(n_workers, dispatcher_url)
    workers.run()