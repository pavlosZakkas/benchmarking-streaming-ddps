import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)


def submit_experiment(master_node, generator_node, args):
    """
    Submits the given experiment as a task on the cluster with the given master node
    """
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
