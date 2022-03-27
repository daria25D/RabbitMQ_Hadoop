# MapReduce word count with RabbitMQ and Redis

To use:
1. Start rabbitmq server with `rabbitmq-server`
2. Start redis server with `redis-server`
3. Run `control.py` with command line arguments for
   1. number of mappers (`-n`, `--mappers`)
   2. number of reducers (`-k`, `--reducers`)
   3. data directory (`-d`, by default './data')
4. Obtain results from file `results.txt`