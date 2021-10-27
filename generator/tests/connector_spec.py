import unittest
import mock
import pytest

from generator import connector
from generator.connector import SocketConnectionException

class ConnectorSpec(unittest.TestCase):
  HOST = 'a_host'
  PORT = '1234'

  def setUp(self) -> None:
    self.SOCKET_MOCK = mock.Mock()
    self.CONNECTION_MOCK = mock.Mock()

  def setup_mock(self, socket_mock):
    socket_mock.return_value = self.SOCKET_MOCK
    self.SOCKET_MOCK.bind.return_value = mock.Mock()
    self.SOCKET_MOCK.listen.return_value = mock.Mock()
    self.SOCKET_MOCK.accept.return_value = (self.CONNECTION_MOCK, mock.Mock())

  def setup_failing_mock(self, socket_mock):
    socket_mock.return_value = self.SOCKET_MOCK
    self.SOCKET_MOCK.bind.side_effect = OSError('Failed to connect')

  @mock.patch('socket.socket')
  def should_raise_an_exception_when_socket_fails_to_connect(self, socket_mock):
    self.setup_failing_mock(socket_mock)
    with pytest.raises(SocketConnectionException):
      connector.wait_for_connection_to(self.HOST, self.PORT)

  @mock.patch('socket.socket')
  def should_connect_to_the_given_host_and_port(self, socket_mock):
    self.setup_mock(socket_mock)

    connection, socket = connector.wait_for_connection_to(self.HOST, self.PORT)

    self.SOCKET_MOCK.bind.assert_called_once_with((self.HOST, self.PORT))
    self.assertEqual(socket, self.SOCKET_MOCK)
    self.assertEqual(connection, self.CONNECTION_MOCK)
