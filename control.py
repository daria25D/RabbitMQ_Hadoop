import argparse, pika, sys, threading
from common import *
from splitter import splitter
from mapper import mapper_wrapper
from shuffler import shuffler
from reducer import reducer_wrapper


def parse_arguments():
    parser = argparse.ArgumentParser('RabbitMQ + Hadoop')
    parser.add_argument('-d', '--dir', type=str, help='directory with data', default='./data')
    parser.add_argument('-n', '--mappers', type=int, help='number of mappers', default=1)
    parser.add_argument('-k', '--reducers', type=int, help='number of reducers', default=1)
    args = parser.parse_args()
    if args.mappers < 1 or args.reducers < 1:
        print('Invalid args')
        exit(1)
    return args


def split_map_shuffle(file_name: str, queue_id: int, reducers_cnt: int):
    splitter(file_name, queue_id)
    mapper_wrapper(queue_id)
    shuffler(queue_id, reducers_cnt)


def write_data_to_files(mappers_cnt, file_name=FILE_NAME):
    with open(file_name, 'r') as source:
        all_lines = source.readlines()
    file_names = [f'data/{i}.txt' for i in range(mappers_cnt)]
    num_of_lines = len(all_lines) // mappers_cnt

    for i in range(len(file_names)):
        with open(file_names[i], 'w') as out_file_name:
            out_file_name.writelines(all_lines[i * num_of_lines:(i + 1) * num_of_lines])
            if i == mappers_cnt - 1:
                out_file_name.writelines(all_lines[(i + 1) * num_of_lines:])

    return file_names


def wait_thread_end(t: threading.Thread):
    try:
        t.join()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            sys.exit(1)


class Control:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=HOST,
                blocked_connection_timeout=3
            )
        )
        self.channel = self.connection.channel()
        self.queue_name = get_control_queue_name()
        self.channel.queue_declare(queue=self.queue_name)
        self.data = {}

    def print_data(self, file_name=OUTPUT_FILE_NAME):
        with open(file_name, 'w') as f:
            for key, value in sorted(self.data.items()):
                f.write(key + ', ' + str(value) + '\n')

    def consume(self):
        for method, properties, line in self.channel.consume(self.queue_name, inactivity_timeout=2):
            if method is None or properties is None or line is None:
                print(f'[CNTRL] End of messages')
                self.stop()
                return
            key, value = line.decode().strip().split(',')
            if key in self.data:
                self.data[key] += int(value)
            else:
                self.data[key] = int(value)

    def start(self):
        print(f'[CNTRL] Waiting for data')
        self.consume()

    def stop(self):
        self.channel.stop_consuming()
        self.channel.queue_delete(self.queue_name)
        self.connection.close()


def control_wrapper():
    try:
        cntrl = Control()
        cntrl.start()
        cntrl.print_data()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            cntrl.stop()
            sys.exit(0)
        except SystemExit:
            sys.exit(1)


def main():
    args = parse_arguments()
    print(args)
    file_names = write_data_to_files(args.mappers)
    # # possible alternative to threading: multiprocessing
    # with multiprocessing.Pool(args.mappers) as mapper:
    # ...
    # with multiprocessing.Pool(args.reducers) as reducer:
    # ...

    threads_n = [] # split -> map -> shuffle
    for i in range(args.mappers):
        threads_n.append(threading.Thread(target=split_map_shuffle, args=(file_names[i], i, args.reducers)))
    for t in threads_n:
        t.start()
    # wait till the end
    for t in threads_n:
        wait_thread_end(t)

    threads_k = [] # reduce
    for i in range(args.reducers):
        threads_k.append(threading.Thread(target=reducer_wrapper, args=(i,)))
    for t in threads_k:
        t.start()
    # wait till the end
    for t in threads_k:
        wait_thread_end(t)
    control_wrapper()


if __name__ == '__main__':
    main()