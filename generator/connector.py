import socket


class SocketConnectionException(Exception):
    pass


def wait_for_connection_to(host, port):
    """
    Opens a socket at the given host and port, waits for the client
    system to connect, and returns tha socket connection
    """
    try:
        stream_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stream_socket.bind((host, port))
        stream_socket.listen()
        connection, addr = stream_socket.accept()
        return connection, stream_socket

    except Exception as e:
        raise SocketConnectionException('Failed to set up connection with the streaming system', e)
