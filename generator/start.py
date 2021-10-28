import time

from generator.connector import wait_for_connection_to, SocketConnectionException
from generator.streaming.streamers import stream_purchase, DataStreamingException
from generator.streaming.methods import schedule_constant_streaming
from multiprocessing import Process


def start_streaming(streaming_interval, messages_per_generator, connection):
    schedule_constant_streaming(
        streaming_interval,
        messages_per_generator,
        stream_purchase,
        connection
    )


def start_generator_instances(
        generators,
        total_messages,
        available_time_in_secs,
        socket,
        connection
):
    messages_per_generator = int(total_messages / generators)
    streaming_interval = available_time_in_secs / messages_per_generator

    try:
        if generators == 1:
            start_streaming(streaming_interval, messages_per_generator, connection)
        else:
            for i in range(generators):
                Process(
                    target=start_streaming,
                    args=(streaming_interval, messages_per_generator, connection),
                    daemon=False
                ).start()
    except Exception as e:
        print('Error in generator instances', e)
    finally:
        connection.close()
        socket.close()


def start(
        host,
        port,
        total_messages,
        available_time_in_secs,
        generators,
        wait_time
):
    try:
        connection, socket = wait_for_connection_to(host, port)
        start_generator_instances(
            generators,
            total_messages,
            available_time_in_secs,
            socket,
            connection
        )
        time.sleep(wait_time)
    except SocketConnectionException or DataStreamingException as e:
        print(e)
    except Exception as e:
        print('Unknown Exception', e)
