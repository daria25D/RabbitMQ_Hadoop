import json
import pika
import sys

from common import HOST, get_reduce_queue_name, get_control_queue_name


def decode_key_val(line: str):
    decoded_line = line.decode().split(':')
    key = decoded_line[0].strip("'")
    value = json.loads(decoded_line[1])
    return key, value


class Reducer:
    def __init__(self, reducer_id):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
        self.channel = self.connection.channel()
        self.queue_name = get_reduce_queue_name(reducer_id)
        self.channel.queue_declare(queue=self.queue_name)
        self.reducer_id = reducer_id

        self.cntrl_connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
        self.cntrl_channel = self.cntrl_connection.channel()
        self.cntrl_queue_name = get_control_queue_name()
        self.cntrl_channel.queue_declare(queue=self.cntrl_queue_name)

    def consume(self):
        for method, properties, line in self.channel.consume(self.queue_name, inactivity_timeout=1):
            if method is None or properties is None or line is None:
                print(f'[R{self.reducer_id}] End of messages')
                self.stop()
                return
            key, value = decode_key_val(line)
            cnt = 0
            for v in value:
                cnt += v
            # print(f'[R{self.reducer_id}] {key}, {cnt}')
            l = key + ',' + str(cnt)
            self.publish(l)

    def publish(self, line):
        self.cntrl_channel.basic_publish(
            exchange='',
            routing_key=self.cntrl_queue_name,
            body=line
        )

    def start(self):
        print(f'[R{self.reducer_id}] Waiting for data')
        self.consume()

    def stop(self):
        self.channel.stop_consuming()
        self.channel.queue_delete(self.queue_name)
        self.connection.close()
        self.cntrl_connection.close()


def reducer_wrapper(reducer_id: int):
    try:
        reducer = Reducer(reducer_id)
        reducer.start()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            reducer.stop()
            sys.exit(0)
        except SystemExit:
            sys.exit(1)
