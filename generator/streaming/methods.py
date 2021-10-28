import sched
import time

SCHED_PRIORITY = 1


def stream_in_constant_intervals(scheduler, interval_in_secs, times, stream_function, connection):
    if times == 0:
        return

    stream_function(connection)
    scheduler.enter(
        interval_in_secs,
        SCHED_PRIORITY,
        stream_in_constant_intervals,
        (scheduler, interval_in_secs, times - 1, stream_function, connection)
    )


def schedule_constant_streaming(interval_in_secs, times, stream_function, connection):
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(
        interval_in_secs,
        SCHED_PRIORITY,
        stream_in_constant_intervals,
        (scheduler, interval_in_secs, times, stream_function, connection)
    )
    scheduler.run()
