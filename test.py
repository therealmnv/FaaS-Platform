import redis
import uuid
import json

class Redis:
    def __init__(self, host = 'localhost', port = '6379') -> None:
        self.redis_client = redis.Redis(host=host, port=port)
        
    def get_client(self):
        return self.redis_client
    
redis_client = Redis().get_client()

key = str(uuid.uuid4())
data = {
    "function": "hai",
    "args": "bai",
    "status": None,
    "result": None
}

# redis_client.set(key, json.dumps(data))

# print(key)

# print(redis_client.get(key))


# for key in redis_client.scan_iter():
#     print (key)
#     print (redis_client.get(key))


import requests
