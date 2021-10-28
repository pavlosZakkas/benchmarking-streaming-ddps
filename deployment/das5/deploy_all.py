import os
import argparse
import subprocess
import time

GENERATOR_NODE = 1

RESERVATION_ID_INDEX = 0
RESERVATION_STATUS_INDEX = 6
FIRST_RESERVED_NODE_INDEX = 8
RESERVATION_POLLING_TIME = 3
RESERVATION_MAX_WAIT_TIME = 30

CLUSTER_READY_POLLING_TIME = 3
CLUSTER_READY_MAX_WAIT_TIME = 60

EXPERIMENT_MAX_WAIT_TIME = 13 * 60

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)

def parsed_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('--nodes', help='Nodes to be used')
  parser.add_argument('--streamPort', help='Streamer port')
  parser.add_argument('--generators', help='Instances of generators to send messages')
  parser.add_argument('--messages', help='Number of messages to be streamed by generator')
  parser.add_argument('--time', help='Time slot to stream the total number of messages')
  parser.add_argument('--experiment', help='Experiment to be executed on system under test')
  parser.add_argument('--sut', help='System under test ("spark" or "flink")')
  parser.add_argument('--waitTime', help='Time to wait till experiment is executed')
  return parser.parse_args()

def reserve_nodes_with(num_of_workers):
  os.system("preserve -# " + str(num_of_workers + GENERATOR_NODE) + " -t 00:15:00")

# Returns the reservation info as a list
def get_reservation():
  reservation_result = subprocess.check_output("preserve -llist | grep ddps2107", shell=True).split()
  return list(map(lambda x: x.decode('utf8'), reservation_result))

def get_reserved_nodes(num_of_workers):
  reserve_nodes_with(num_of_workers)
  status = get_reservation()[RESERVATION_STATUS_INDEX]
  waiting_time = 0
  while status != 'R' and waiting_time < RESERVATION_MAX_WAIT_TIME:
    status = get_reservation()[RESERVATION_STATUS_INDEX]
    waiting_time += RESERVATION_POLLING_TIME
    time.sleep(RESERVATION_POLLING_TIME)

  if status != 'R':
    print(f'Could not reserve nodes after {RESERVATION_MAX_WAIT_TIME} seconds')
    exit(1)

  return get_reservation()[FIRST_RESERVED_NODE_INDEX:]

def deploy_generator(args, node):
  os.system(
    f'ssh {node}'
    f' /home/ddps2107/Soft/miniconda3/bin/python '
    f' {project_dir}/generator/deployment/start_generator.py'
    f' --host {node}'
    f' --port {args.streamPort}'
    f' --generators {args.generators}'
    f' --messages {args.messages}'
    f' --time {args.time} '
    f' --waitTime {args.waitTime} &'
  )
  return

def deploy_cluster(args, sut_nodes):
  master_node = sut_nodes[0]
  if args.sut == 'spark':
    os.system(
      f'ssh {master_node}'
      f' /home/ddps2107/Soft/miniconda3/bin/python'
      f' {project_dir}/system_under_test/spark/deployment/start_cluster.py '
      f' --nodes {" ".join(sut_nodes)}'
      # f' source /home/ddps2107/Soft/miniconda3/bin/activate &&'
      # f' python {project_dir}/system_under_test/spark/deployment/start_cluster.py --nodes {" ".join(sut_nodes)}'
    )
  return

def is_spark_ready(master_node):
  result = subprocess.check_output(
    f'ssh {master_node}'
    f' /home/ddps2107/Soft/miniconda3/bin/python '
    f' {project_dir}/system_under_test/spark/deployment/is_cluster_live.py',
    shell=True
  )

  print(str(result))
  if 'live' in str(result):
    return True
  else:
    return False

def wait_for_ready_cluster(master_node):
  if args.sut == 'spark':
    waiting_time = 0
    while not is_spark_ready(master_node) and waiting_time > CLUSTER_READY_MAX_WAIT_TIME:
      waiting_time += CLUSTER_READY_POLLING_TIME
      time.sleep(CLUSTER_READY_POLLING_TIME)

    if is_spark_ready(master_node):
      return True
    else:
      print('Spark cluster could not be deployed')
      exit(1)

def submit_experiment(master_node, generator_node, args):
  if args.sut == 'spark':
    os.system(
      f'ssh {master_node}'
      f' /home/ddps2107/Soft/miniconda3/bin/python '
      f' {project_dir}/system_under_test/spark/deployment/submit_task.py'
      f' --experiment {args.experiment}'
      f' --streamHost {generator_node}'
      f' --streamPort {args.streamPort}'
      f' --masterNode {master_node} &'
    )

def stop_cluster(master_node):
  if args.sut == 'spark':
    os.system(
      f'ssh {master_node}'
      f' /home/ddps2107/Soft/miniconda3/bin/python'
      f' {project_dir}/system_under_test/spark/deployment/stop_cluster.py '
    )
  return

def cancel_reservation():
  reservation_id = get_reservation()[RESERVATION_ID_INDEX]
  os.system("preserve -c " + reservation_id)

def deploy_all(args):
  nodes = get_reserved_nodes(int(args.nodes))
  print(f'Nodes {", ".join(nodes)} were reserved')
  generator_node = nodes[0]
  sut_nodes = nodes[GENERATOR_NODE:]
  master_node = sut_nodes[0]
  print(f'Generator node: {generator_node}')
  print(f'SUT nodes: {", ".join(sut_nodes)}')

  print('Deploying generator...')
  deploy_generator(args, generator_node)
  print('Generator was deployed!')

  print('Starting SUT cluster')
  if not is_spark_ready(master_node):
    deploy_cluster(args, sut_nodes)

  wait_for_ready_cluster(master_node)
  print('SUT cluster is live')

  print(f'Submitting experiment{args.experiment}')
  submit_experiment(master_node, generator_node, args)

  wait_time = int(args.waitTime) if args.waitTime else EXPERIMENT_MAX_WAIT_TIME
  print(f'Waiting {wait_time} seconds for the experiment to run')
  time.sleep(wait_time)
  print('Waiting time is over')

  print('Stopping SUT cluster')
  stop_cluster(master_node)

  print("Cancelling reservation")
  cancel_reservation()

if __name__ == "__main__":
  args = parsed_arguments()
  deploy_all(args)
