import os
import argparse
from datetime import datetime

if os.environ.get('DEPLOY_ENV') == 'LOCAL':
    import local_env as env
else:
    import das5_env as env

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)

executable_per_experiment = {
    'aggregationRDD': f'aggregator.PurchaseAggregatorRDD',
    'aggregationLargeWindowRDD': f'aggregator.PurchaseAggregatorLargeWindowRDD',
}


def parsed_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--experiment', help='Experiment to be executed')
    parser.add_argument('--masterNode', help='Master node host')
    parser.add_argument('--streamHost', help='Streamer host')
    parser.add_argument('--streamPort', help='Streamer port')
    return parser.parse_args()


def submit_task(args):
    os.chdir(env.SPARK_HOME)
    os.system(f'./bin/spark-submit '
              f'--master spark://{args.masterNode}:{env.SPARK_MASTER_PORT}  '
              f'--class {executable_per_experiment[args.experiment]} '
              f'{parent_dir}/target/scala-2.12/spark_2.12-0.1.jar  '
              f'{args.streamHost or env.STREAMER_HOST} '
              f'{args.streamPort or env.STREAMER_POST} '
              f'{parent_dir}/results/{args.experiment}/event-latency_{datetime.now().strftime("%Y-%m-%d_%H:%M")}/event-latency'
              f' >> {parent_dir}/results/{args.experiment}/processing-latency-{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}')


if __name__ == "__main__":
    args = parsed_arguments()
    if args.experiment not in executable_per_experiment:
        print(f'No executable found for {args.experiment}')
        exit(1)

    submit_task(args)
