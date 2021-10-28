import unittest
import mock
from generator.streaming import methods


class StreamingMethodsSpec(unittest.TestCase):
    def setUp(self) -> None:
        self.SCHEDULER_MOCK = mock.Mock()
        self.INTERVAL = 1
        self.TEN_TIMES = 10
        self.NINE_TIMES = 9
        self.ZERO_TIMES = 0
        self.STREAM_FUNCTION = mock.Mock()
        self.CONNECTION = mock.Mock()

    def setup_mocks(self, sched):
        sched.return_value = self.SCHEDULER_MOCK
        self.SCHEDULER_MOCK.enter.return_value = mock.Mock()
        self.SCHEDULER_MOCK.run = mock.Mock()

    @mock.patch('generator.streaming.methods.sched.scheduler')
    def should_schedule_constant_streaming(self, sched_mock):
        # given
        self.setup_mocks(sched_mock)

        # when
        methods.schedule_constant_streaming(self.INTERVAL, self.TEN_TIMES, self.STREAM_FUNCTION, self.CONNECTION)

        # then
        self.SCHEDULER_MOCK.enter.assert_called_once_with(
            self.INTERVAL,
            1,
            methods.stream_in_constant_intervals,
            (self.SCHEDULER_MOCK, self.INTERVAL, self.TEN_TIMES, self.STREAM_FUNCTION, self.CONNECTION)
        )

    def should_stream_in_constant_intervals_by_using_scheduler(self):
        # when
        methods.stream_in_constant_intervals(
            self.SCHEDULER_MOCK,
            self.INTERVAL,
            self.TEN_TIMES,
            self.STREAM_FUNCTION,
            self.CONNECTION
        )

        # then
        self.STREAM_FUNCTION.assert_called_once_with(self.CONNECTION)
        self.SCHEDULER_MOCK.enter.assert_called_once_with(
            self.INTERVAL,
            1,
            methods.stream_in_constant_intervals,
            (self.SCHEDULER_MOCK, self.INTERVAL, self.NINE_TIMES, self.STREAM_FUNCTION, self.CONNECTION)
        )

    def should_stop_constant_streaming_when_there_are_no_times_left(self):
        # when
        methods.stream_in_constant_intervals(
            self.SCHEDULER_MOCK,
            self.INTERVAL,
            self.ZERO_TIMES,
            self.STREAM_FUNCTION,
            self.CONNECTION
        )

        # then
        self.STREAM_FUNCTION.assert_not_called()
        self.SCHEDULER_MOCK.enter.assert_not_called()
