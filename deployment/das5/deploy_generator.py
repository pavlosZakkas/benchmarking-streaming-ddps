import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)


def deploy_generator(args, node):
    """
    Triggers the execution of the generator on the given node
    """
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
