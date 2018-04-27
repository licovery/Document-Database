from sortedcontainers import SortedDict

class CachingMiddleware(object):

    WRITE_CACHE_SIZE = 1000

    def __init__(self, storage_cls):
        self._db_cache = {}
        self._index_cache = SortedDict()
        self._cache_modified_count = 0
        self._storage_cls = storage_cls

    def __call__(self, name):
        self.storage = self._storage_cls(name)
        self._db_cache, self._index_cache = self.storage.read()
        return self


    #析构的时候刷新缓冲区
    def close (self):
        self.flush()
        self.storage.close()





    def write(self, data, index):
        self._db_cache = data
        self._index_cache = index

        self._cache_modified_count += 1

        if self._cache_modified_count >= self.WRITE_CACHE_SIZE:
            self.flush()

    #
    def read(self):
        # if self._db_cache == None and self._index_cache == None:
        #     self._db_cache, self._index_cache = self.storage.read()
        return self._db_cache, self._index_cache


    #立即刷新缓冲区
    def flush(self):
        self.storage.write(self._db_cache, self._index_cache)
        self._cache_modified_count = 0

