import os
import redis
from contextlib import contextmanager

class RedisHelper:
    def __init__(self):
        self._conn = None

    def get_conn_str(self):
        # Initialisation
        conn_str = "localhost"
        if "REDIS_URL" in os.environ:
            conn_str = os.getenv("REDIS_URL")

        return conn_str

    def initialize_connection(self):
        conn_str = self.get_conn_str()
        self._conn = redis.Redis(host=conn_str, port=6379, db=0)

    @contextmanager
    def get_resource(self):
        if self._conn is None:
            self.initialize_connection()
        yield self._conn


redis_helper = RedisHelper()