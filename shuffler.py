import pika
import redis

from common import HOST, get_redis_id_name, get_reduce_queue_name


def decode_key_val(line: str):
    decoded_line = line.decode().strip('][').split(',')
    key = decoded_line[0].strip("'")
    value = int(decoded_line[1])
    return key, value


def shuffler(queue_id: int, reducers_cnt: int):
    print(f'[SH{queue_id}] Waiting for data')
    r = redis.Redis(host=HOST, port=6379, db=0)
    redis_id = get_redis_id_name(queue_id)
    data = r.lrange(redis_id, 0, -1)
    data_to_send = {}
    for d in data:
        key, value = decode_key_val(d)
        if key in data_to_send:
            data_to_send[key].append(value)
        else:
            data_to_send[key] = [value]
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channels = []
    queue_names = []
    for i in range(reducers_cnt):
        channels.append(connection.channel())
        queue_names.append(get_reduce_queue_name(i))
    for ch, q in zip(channels, queue_names):
        ch.queue_declare(queue=q)
    reducer_id = 0
    for key, value in data_to_send.items():
        line = key + ':' + str(value)
        # print(line)
        channels[reducer_id].basic_publish(
            exchange='',
            routing_key=queue_names[reducer_id],
            body=line
        )
        reducer_id = (reducer_id + 1) % reducers_cnt
    connection.close()
