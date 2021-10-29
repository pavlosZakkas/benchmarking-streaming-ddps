import argparse
import time
import sys, os

current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)

from deployment.das5.deploy_generator import deploy_generator
from deployment.das5.nodes_reservation import get_reserved_nodes, cancel_reservation
from deployment.das5.spark_cluster_handler import is_spark_ready, deploy_cluster, wait_for_ready_cluster, stop_cluster
from deployment.das5.submit_spark_experiment import submit_experiment

GENERATOR_NODE = 1

RESERVATION_ID_INDEX = 0
RESERVATION_STATUS_INDEX = 6
FIRST_RESERVED_NODE_INDEX = 8
RESERVATION_POLLING_TIME = 3
RESERVATION_MAX_WAIT_TIME = 30

CLUSTER_READY_POLLING_TIME = 3
CLUSTER_READY_MAX_WAIT_TIME = 60

EXPERIMENT_MAX_WAIT_TIME = 13 * 60


def parsed_arguments():
    """
    Parses arguments to be used for allocating nods, deploying generator and spark cluster and run the experiment
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodes', help='Nodes to be used')
    parser.add_argument('--streamPort', help='Streamer port')
    parser.add_argument('--generators', help='Instances of generators to send messages')
    parser.add_argument('--messages', help='Number of messages to be streamed by generator')
    parser.add_argument('--time', help='Time slot to stream the total number of messages')
    parser.add_argument('--experiment', help='Experiment to be executed on system under test')
    parser.add_argument('--sut', help='System under test ("spark" is currently supported)')
    parser.add_argument('--waitTime', help='Time to wait till experiment is executed')
    return parser.parse_args()


def run_experiment(args):
    """
    Reserves nodes, deploys generator, deploys spark cluster,
    submits spark task for experiment, releases reserved nodes
    """
    nodes = get_reserved_nodes(int(args.nodes), GENERATOR_NODE)
    print(f'Nodes {", ".join(nodes)} were reserved')
    generator_node = nodes[0]
    sut_nodes = nodes[GENERATOR_NODE:]
    master_node = sut_nodes[0]
    print(f'Generator node: {generator_node}')
    print(f'SUT nodes: {", ".join(sut_nodes)}')

    try:
        print('Deploying generator...')
        deploy_generator(args, generator_node)
        print('Generator was deployed!')

        print('Starting SUT cluster')
        if not is_spark_ready(master_node):
            deploy_cluster(args, sut_nodes)

        wait_for_ready_cluster(master_node, args)
        print('SUT cluster is live')

        print(f'Submitting experiment{args.experiment}')
        submit_experiment(master_node, generator_node, args)

        wait_time = int(args.waitTime) if args.waitTime else EXPERIMENT_MAX_WAIT_TIME
        print(f'Waiting {wait_time} seconds for the experiment to run')
        time.sleep(wait_time)
        print('Waiting time is over')
    except KeyboardInterrupt:
        print('Experiment was interrupted')
    finally:
        print('Stopping SUT cluster')
        stop_cluster(master_node, args)
        print("Cancelling reservation")
        cancel_reservation()


if __name__ == "__main__":
    args = parsed_arguments()
    run_experiment(args)
