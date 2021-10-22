import unittest

class DummySpec(unittest.TestCase):
  def should_pass(self):
    self.assertEqual(True, True)
