import time
import numpy as np
from numpy import random

from generator.streaming import dataset

GEM_PACK_ID_MEAN = 1000
GEM_PACK_ID_STD = 1
PRICE_MEAN = 100
PRICE_STD = 10


def gem_pack_id():
    """
    Generates a gem pack id, by taking values from a normal distribution
    """
    return int(np.round(random.normal(GEM_PACK_ID_MEAN, GEM_PACK_ID_STD)))


def stream_purchase(connection):
    """
    Streams a generated purchase to the given connection
    """
    purchase = dataset.PurchaseBuilder() \
        .with_gem_pack_id(gem_pack_id()) \
        .with_price(random.normal(PRICE_MEAN, PRICE_STD)) \
        .with_event_time_in_milliseconds(int(time.time_ns() / 1e6)) \
        .build() \
        .to_event()

    try:
        connection.sendall(purchase.encode())
    except Exception as e:
        raise DataStreamingException('Error while publishing to socket', e)


class DataStreamingException(Exception):
    pass
