from dataclasses import dataclass
import time

@dataclass
class Purchase():
  user_id: int
  gem_pack_id: int
  price: float
  event_time_in_nanosecs: int

  def to_tuple(self):
    return (self.user_id, self.gem_pack_id, self.price, self.event_time_in_nanosecs)

class PurchaseBuilder():
  def __init__(self):
    self.purchase = Purchase(
      user_id=1,
      gem_pack_id=1,
      price=10,
      event_time_in_nanosecs=time.time_ns()
    )

  def with_user_id(self, user_id):
    self.purchase.user_id = user_id
    return self

  def with_gem_pack_id(self, gem_pack_id):
    self.purchase.gem_pack_id = gem_pack_id
    return self

  def with_price(self, price):
    self.purchase.price = price
    return self

  def with_event_time_in_nanosecs(self, event_time_in_nanosecs):
    self.purchase.event_time_in_nanosecs = event_time_in_nanosecs
    return self

  def build(self):
    return self.purchase
