from dataclasses import dataclass
import time


@dataclass
class Purchase:
    """
    Data object for Purchase
    User id is present in Purchase, but is not needed for our experiments
    as the aggregation will be done on price, after grouping by the gem pack id.
    Event time in milliseconds is also added in order to calculate event time latency
    """
    user_id: int
    gem_pack_id: int
    price: float
    event_time_in_milliseconds: int

    def to_event(self):
        """
        Creates the event to be sent for the given Purchase
        """
        return f'{self.gem_pack_id},{self.event_time_in_milliseconds},{self.price}\n'


class PurchaseBuilder:
    """
    PurchaseBuilder is an implementation of the Builder Design Pattern for
    the Purchase object
    """

    def __init__(self):
        self.purchase = Purchase(
            user_id=1,
            gem_pack_id=1,
            price=10,
            event_time_in_milliseconds=int(time.time_ns() / 1e6)
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

    def with_event_time_in_milliseconds(self, event_time_in_milliseconds):
        self.purchase.event_time_in_milliseconds = event_time_in_milliseconds
        return self

    def build(self):
        return self.purchase
