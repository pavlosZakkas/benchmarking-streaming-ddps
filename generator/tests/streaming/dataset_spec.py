import unittest

from generator.streaming.dataset import PurchaseBuilder, Purchase

class DatasetSpec(unittest.TestCase):
  USER_ID = 1
  GEM_PACK_ID = 10
  PRICE = 100
  EVENT_TIME = 1000

  def should_build_a_purchase_object(self):
    # when
    purchase = PurchaseBuilder() \
      .with_user_id(self.USER_ID) \
      .with_gem_pack_id(self.GEM_PACK_ID) \
      .with_price(self.PRICE) \
      .with_event_time_in_milliseconds(self.EVENT_TIME) \
      .build()

    # then
    assert purchase.user_id == self.USER_ID
    assert purchase.gem_pack_id == self.GEM_PACK_ID
    assert purchase.price == self.PRICE
    assert purchase.event_time_in_milliseconds == self.EVENT_TIME

  def should_create_a_tuple_from_a_purchase_object(self):
    # given
    purchase = Purchase(
      user_id=self.USER_ID,
      gem_pack_id=self.GEM_PACK_ID,
      price=self.PRICE,
      event_time_in_milliseconds=self.EVENT_TIME
    )

    # when
    purchase_event = purchase.to_event()

    # then
    assert purchase_event == f'{self.GEM_PACK_ID},{self.EVENT_TIME},{self.PRICE}\n'
