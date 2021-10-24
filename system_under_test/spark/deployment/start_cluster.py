import os
import env
import argparse

SPARK_ENV_PATH = f'{env.SPARK_HOME}/conf/spark-env.sh'
SPARK_WORKERS_PATH = f'{env.SPARK_HOME}/conf/workers.sh'

def setup_spark_env():
  os.truncate(SPARK_ENV_PATH, 0)
  os.system(f'echo "export SPARK_MASTER_HOST={env.SPARK_MASTER_HOST}" >> {SPARK_ENV_PATH}')
  os.system(f'echo "export JAVA_HOME={env.JAVA_HOME}" >> {SPARK_ENV_PATH}')
  os.system(f'echo "export SPARK_MASTER_WEBUI_PORT={env.SPARK_MASTER_WEBUI_PORT}" >> {SPARK_ENV_PATH}')
  os.system(f'echo "export SPARK_WORKER_CORES={env.SPARK_WORKER_CORES}" >> {SPARK_ENV_PATH}')
  os.system(f'echo "export SPARK_WORKER_INSTANCES={env.SPARK_WORKER_INSTANCES}" >> {SPARK_ENV_PATH}')
  os.system(f'echo "export SPARK_WORKER_MEMORY={env.SPARK_WORKER_MEMORY}" >> {SPARK_ENV_PATH}')
  os.system(f'echo "export PYSPARK_PYTHON={env.PYSPARK_PYTHON}" >> {SPARK_ENV_PATH}')
  os.system(f'echo "export PYSPARK_DRIVER_PYTHON={env.PYSPARK_DRIVER_PYTHON}" >> {SPARK_ENV_PATH}')

def setup_workers(nodes):
  os.truncate(SPARK_WORKERS_PATH, 0)
  for node in nodes:
    os.system(f'echo {node} >> {SPARK_WORKERS_PATH}')

def parsed_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('--nodes', nargs='+', help='Nodes of spark cluster', required=True)
  return parser.parse_args()

def start_cluster():
  args = parsed_arguments()
  setup_spark_env()
  setup_workers(args.nodes)
  os.chdir(f'{env.SPARK_HOME}')
  os.system(f'./sbin/start-all.sh')


if __name__ == "__main__":
  start_cluster()
