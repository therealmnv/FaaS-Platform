import sys
import zmq
import multiprocessing as mp

import dill
import codecs


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


class PushWorkers:
    def __init__(self, num_worker_processors, dispatcher_url):
        self.num_worker_processors = num_worker_processors
        self.address = dispatcher_url
        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)

    def run(self):
        with mp.Pool(processes=self.num_worker_processors) as pool:
            while True:
                id, task = self._receive_task()
                task_object = deserialize(task)
                params = eval(task_object.params)
                result_obj = pool.apply(task_object.fn, (params,))
                result = serialize(result_obj)
                self._send_result(id, result)

    def _receive_task(self):
        id_bytes, data_bytes = self.socket.recv_multipart()
        task_id = id_bytes.decode('utf-8')
        task_data = data_bytes.decode('utf-8')
        return task_id, task_data
    
    def _send_result(self, task_id, task_result):
        id_bytes = task_id.encode('utf-8')
        result_bytes = task_result.encode('utf-8')
        self.socket.send_multipart([id_bytes, result_bytes])



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Please provide fields: <num_worker_processors> <dispatcher url>")
        sys.exit(1)

    num_worker_processors = int(sys.argv[1])
    dispatcher_url = sys.argv[2]

    workers = PushWorkers(num_worker_processors, dispatcher_url)
    workers.run()