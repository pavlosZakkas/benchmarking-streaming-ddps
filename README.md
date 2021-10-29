# Benchmarking Streaming Distributed Benchmarking Systems

In this repository we were trying to reproduce experiments made by [Karimov et al. article](https://arxiv.org/pdf/1802.08496.pdf).

More information about what we did and how it differ from article authors work can be read in the report in the repository.


## Data generator

Tests execution:
~~~
$ cd generator
$ pytest
~~~

Start generator:
~~~
$ python generator/deployment/start_generator.py --messages <messages> --time <time_in_secs> --host <host> --generators <generator_processes> --port <port> --waitTime <wait_time before exiting>
~~~

## Spark

Package to JAR:
~~~
$ cd system_under_test/spark
# sbt package
~~~

Start cluster with specified cluster nodes:
~~~
$ cd system_under_test/spark
$ python deployment/start_cluster.py --nodes <master_node> <worker_node1> <worker_node_n>
~~~

Stop cluster:
~~~
$ cd system_under_test/spark
$ python deployment/stop_cluster.py
~~~

Check if there is a live cluster:
~~~
$ cd system_under_test/spark
$  python deployment/is_cluster_live.py
~~~

## Experiments execution 
Experiments were run on DAS-5 cluster as ddps2107 user with the following configurable command:

~~~
$ python3 deployment/das5/run_experiment.py --nodes 4 --messages 100000000 --time 600 --generators 32 --streamPort 9997 --experiment aggregationRDD --sut spark --waitTime 900
~~~
