import sys
import threading
import zmq

import dill
import codecs


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


class PullWorkers:
    def __init__(self, n_workers, dispatcher_url):
        self.n_workers = n_workers
        self.address = dispatcher_url
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)

    def run(self):
        self.threads = [threading.Thread(target=self._run_worker, name = idx) 
                        for idx in range(self.n_workers)]
        for thread in self._threads:
            thread.start()

    def _run_worker(self):
        result = ""
        while True:
            task = self._request_task(result)
            task_object = deserialize(task)
            params = eval(task_object.params)
            result_obj = task_object.fn(*params)
            result = serialize(result_obj)

    def _request_task(self, task_string):
        self.socket.send_string(task_string)
        response = self.socket.recv_string()
        return response


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Please provide fields: <num_worker_processors> <dispatcher url>")
        sys.exit(1)

    n_workers = int(sys.argv[1])
    dispatcher_url = sys.argv[2]

    workers = PullWorkers(n_workers, dispatcher_url)
    workers.run()


    