from .query import where
from .storage import Storage
from .database import DocDb
from .middleware import CachingMiddleware


#增加上下文管理协议
#python 内存管理  __del__有坑

__all__ = ('DocDb', 'Storage', 'where', 'CachingMiddleware')
