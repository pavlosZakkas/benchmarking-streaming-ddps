import os
import subprocess
import time

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)

CLUSTER_READY_POLLING_TIME = 3
CLUSTER_READY_MAX_WAIT_TIME = 60


def deploy_cluster(args, sut_nodes):
    """
    Deploys a spark cluster on the given set of nodes
    """
    master_node = sut_nodes[0]
    if args.sut == 'spark':
        os.system(
            f'ssh {master_node}'
            f' /home/ddps2107/Soft/miniconda3/bin/python'
            f' {project_dir}/system_under_test/spark/deployment/start_cluster.py '
            f' --nodes {" ".join(sut_nodes)}'
        )
    return


def is_spark_ready(master_node):
    """
    Returns whether a spark cluster is running on the given master node
    """
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


def wait_for_ready_cluster(master_node, args):
    """
    Waits till the spark cluster on the given master node is live
    """
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


def stop_cluster(master_node, args):
    """
    Stops the spark cluster running with the given master node
    """
    if args.sut == 'spark':
        os.system(
            f'ssh {master_node}'
            f' /home/ddps2107/Soft/miniconda3/bin/python'
            f' {project_dir}/system_under_test/spark/deployment/stop_cluster.py '
        )
    return
