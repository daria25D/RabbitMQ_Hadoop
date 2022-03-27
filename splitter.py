import pika
# import threading
from common import HOST, get_queue_name


def splitter(file_name: str, queue_id: int):
    # establish connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()
    # create queue (if it was not yet created), smth process id dependent?
    queue_name = get_queue_name(queue_id)
    channel.queue_declare(queue=queue_name)
    # publish lines to queue for mapper to read
    num_line = 0
    print(f'[SP{queue_id}] Starting sending lines')
    with open(file_name, 'r') as f:
        for line in f:
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=line
            )
            # DEBUG
            # if num_line < 5:
            #     print(f'Line {num_line} {line[:50]}... sent to mapper')
            #     num_line += 1
    connection.close()
