import sys
import zmq
import multiprocessing as mp

import dill
import codecs


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


class PullWorkers:
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
    if len(sys.argv) != 3:
        print("Please provide fields: <num_worker_processors> <dispatcher url>")
        sys.exit(1)

    num_worker_processors = int(sys.argv[1])
    dispatcher_url = sys.argv[2]

    workers = PullWorkers(num_worker_processors, dispatcher_url)
    workers.run()


    