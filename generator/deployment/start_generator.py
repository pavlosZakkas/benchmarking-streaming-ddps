import argparse
import env
import os, sys

current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)
from generator.start import start

def parsed_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('--messages', help='Number of messages to stream')
  parser.add_argument('--time', help='Time threshold in seconds to stream the messages')
  parser.add_argument('--host', help='Host of the socket')
  parser.add_argument('--port', help='Port of the socket')
  parser.add_argument('--generators', help='Number of generators')
  parser.add_argument('--waitTime', help='Time in seconds to wait before exiting')
  return parser.parse_args()

def start_generator(args):
  start(
    host=args.host or env.DEFAULT_GENERATOR_HOST,
    port=int(args.port) if args.port else env.DEFAULT_GENERATOR_PORT,
    total_messages=int(args.messages) if args.messages else env.DEFAULT_NUMBER_OF_MESSAGES,
    available_time_in_secs=int(args.time) if args.time else env.DEFAULT_TIME_IN_SECS,
    generators=int(args.generators) if args.generators else env.DEFAULT_NUMBER_OF_GENERATORS,
    wait_time=int(args.waitTime) if args.waitTime else env.DEFAULT_WAIT_TIME_IN_SECS
  )

if __name__ == "__main__":
  args = parsed_arguments()
  start_generator(args)
