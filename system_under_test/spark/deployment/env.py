import pathlib

PATH_TO_SPARK_MODULE = pathlib.Path(__file__).parent.parent.resolve()
SPARK_HOME = '/usr/local/spark'
STREAMER_HOST = 'localhost'
STREAMER_POST = '9999'
AGGREGATE_TASK_PATH = '../aggregator.py'

SPARK_MASTER_HOST = 'localhost'
SPARK_MASTER_PORT = 7077
SPARK_MASTER_WEBUI_PORT = 8088
JAVA_HOME = '/Users/pzakkas/.sdkman/candidates/java/11.0.11-zulu'
SPARK_WORKER_CORES = 2
SPARK_WORKER_INSTANCES = 2
SPARK_WORKER_MEMORY = '2g'
PYSPARK_PYTHON = '/Users/pzakkas/.pyenv/shims/python'
PYSPARK_DRIVER_PYTHON = '/Users/pzakkas/.pyenv/shims/python'
