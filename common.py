FILE_NAME = './data/words.txt'
OUTPUT_FILE_NAME = './result.txt'
HOST = 'localhost'
QUEUE_NAME = 'test'


def get_queue_name(queue_id: int):
    return f'queue{queue_id}'


def get_redis_id_name(queue_id: int):
    return f'db{queue_id}'


def get_control_queue_name():
    return 'control'


def get_reduce_queue_name(reducer_id: int):
    return f'reduce{reducer_id}'
