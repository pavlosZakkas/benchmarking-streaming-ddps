import unittest
import mock
import pytest

from generator.streaming.streamers import stream_purchase, DataStreamingException

class StreamersSpec(unittest.TestCase):
  USER_ID = 1
  GEM_PACK_ID = 10
  PRICE = 100
  EVENT_TIME = 1000

  @mock.patch('generator.streaming.streamers.random')
  @mock.patch('generator.streaming.streamers.time')
  def should_send_a_purchase_to_the_connection_socket(self, time_mock, random_mock):
    # given
    connection = mock.Mock()
    time_mock.time_ns.return_value = self.EVENT_TIME
    random_mock.normal.side_effect = [self.GEM_PACK_ID, self.PRICE]

    # when
    stream_purchase(connection)

    # then
    connection.sendall.assert_called_once_with(
      bytes(
        (str((self.USER_ID, self.GEM_PACK_ID, self.PRICE, self.EVENT_TIME))+'\n').encode()
      )
    )

  def should_raise_an_exception_when_fails_to_stream_purchase(self):
    connection = mock.Mock()
    connection.sendall.side_effect = Exception('Failed to send purchase')

    with pytest.raises(DataStreamingException):
      stream_purchase(connection)
