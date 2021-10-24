import time
import numpy as np
from numpy import random

from generator.streaming import dataset

GEM_PACK_ID_MEAN = 1000
GEM_PACK_ID_STD = 2
PRICE_MEAN = 100

def stream_purchase(connection):
  purchase = dataset.PurchaseBuilder() \
    .with_gem_pack_id(gem_pack_id()) \
    .with_price(random.normal(PRICE_MEAN, 10)) \
    .with_event_time_in_nanosecs(time.time_ns()) \
    .build() \
    .to_tuple()

  try:
    connection.sendall((str(purchase) + '\n').encode())
  except Exception as e:
    raise DataStreamingException('Error while publishing to socket', e)

def gem_pack_id():
  return int(np.round(random.normal(GEM_PACK_ID_MEAN, GEM_PACK_ID_STD)))

def hello_world(connection):
  try:
    connection.sendall('Hello world\n'.encode())
    print('Sent', time.time_ns())
  except Exception as e:
    raise DataStreamingException('Error while publishing to socket', e)

class DataStreamingException(Exception):
  pass
