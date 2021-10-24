from generator.connector import wait_for_connection_to, SocketConnectionException
from generator.streaming.streamers import stream_purchase, DataStreamingException
from generator.streaming.methods import schedule_constant_streaming

def start(host, port, number_of_messages, available_time_in_secs):
  try:
    connection = wait_for_connection_to(host, port)
    streaming_interval = available_time_in_secs / number_of_messages
    schedule_constant_streaming(streaming_interval, number_of_messages, stream_purchase, connection)
  except SocketConnectionException or DataStreamingException as e:
    print(e)
  except Exception as e:
    print('Unknown Exception', e)
