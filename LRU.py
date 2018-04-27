from collections import OrderedDict

class LRU():

    def __init__(self, capacity=50):
        self._lru = OrderedDict()
        self._capacity = capacity

    def get(self, key):
        if key in self._lru:
            value = self._lru.pop(key)
            self._lru[key] = value
            return value
        else:
            return None

    def put(self, key, value):
        if key not in self._lru and len(self._lru) == self._capacity:
            self._lru.popitem(last=False)

        self._lru[key] = value

    def clear(self):
        self._lru.clear()

    def __contains__(self, item):
        return item in self._lru