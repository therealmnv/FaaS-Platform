import redis

class Redis:
    def __init__(self, host = 'localhost', port = '6379') -> None:
        self.redis_client = redis.Redis(host=host, port=port)
        
    def get_client(self):
        return self.redis_client