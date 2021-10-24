import os
import env

def stop_cluster():
  os.chdir(f'{env.SPARK_HOME}')
  os.system(f'./sbin/stop-all.sh')

if __name__ == "__main__":
  stop_cluster()
