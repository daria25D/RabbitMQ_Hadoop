import pika
import redis
import sys

from common import HOST, get_queue_name, get_redis_id_name


def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate


def normalize_word(word: str):
    w = word.strip().lower() # whitespace characters
    return w.strip(',.') # punctuation


class Mapper:
    def __init__(self, queue_id):
        self.queue_id = queue_id
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=HOST,
                blocked_connection_timeout=1
            )
        )
        self.channel = self.connection.channel()
        self.queue_name = get_queue_name(queue_id)
        self.channel.queue_declare(queue=self.queue_name)
        self.r = redis.Redis(host=HOST, port=6379, db=0)
        self.redis_id = get_redis_id_name(queue_id)
        if self.r.exists(self.redis_id):  # some leftovers?
            print(f'[M{self.queue_id}] Delete old data')
            self.r.delete(self.redis_id)
        self.counter = 0

    def consume(self):
        for method, properties, line in self.channel.consume(self.queue_name, inactivity_timeout=1):
            if method is None or properties is None or line is None:
                print(f'[M{self.queue_id}] End of messages')
                self.stop()
                return
            ws = line.decode('ascii').split(' ')
            for w in ws:
                nw = normalize_word(w)
                if nw != '':
                    self.r.lpush(self.redis_id, str([nw, 1]))
            # if self.counter < 5:
            #     print(f'Line number {self.counter} {ws[:5]}')
            # self.counter += 1

    def start(self):
        print(f'[M{self.queue_id}] Waiting for lines')
        self.consume()

    def stop(self):
        self.channel.stop_consuming()
        self.channel.queue_delete(self.queue_name)
        self.connection.close()


def mapper_wrapper(queue_id: int):
    try:
        mapper = Mapper(queue_id)
        mapper.start()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            mapper.stop()
            sys.exit(0)
        except SystemExit:
            sys.exit(1)