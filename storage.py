
#似乎pickle的性能还挺高的

#ujson的性能更高
try:
    import ujson as json
except:
    import json


import os
from sortedcontainers import SortedDict
import pickle



#新建一个目录，存放索引和json文件

#如何不存在，则创建一个文件
def createFile(fname):
    if not os.path.exists(fname):
        f = open(fname, 'w')
        f.close()



#以json文件格式存储
class Storage(object):
    def __init__(self, path):
        if not os.path.isdir(path):
            os.mkdir(path)
        os.chdir(path)

        self._path = path
        self._db = os.path.basename(path)
        self._index =  self._db + '.index'

        createFile(self._db)
        self._db_handle = open(self._db, 'r+')
        createFile(self._index)
        self._index_handle = open(self._index, 'rb+')


    def close(self):
        # print('storage destory')
        self._db_handle.close()
        self._index_handle.close()

    #初始状态返回一个空字典和空有序字典
    def read(self):

        # 初始数据库为空，json解码会错误
        try:
            self._db_handle.seek(0)
            data = json.load(self._db_handle)
        except:
            data = {}


        #索引为空时，SortedDict会解码错误
        try:
            self._index_handle.seek(0)
            index = pickle.load(self._index_handle)
        except:
            index = SortedDict()


        return data, index


    #新的要创建，旧的要删除
    def write(self, data, index):
        #写数据库

        #重复代码太多

        #素质四连
        self._db_handle.seek(0)
        json.dump(data, self._db_handle)
        self._db_handle.flush()
        self._db_handle.truncate()


        self._index_handle.seek(0)
        pickle.dump(index, self._index_handle)
        self._index_handle.flush()
        self._index_handle.truncate()

        #写索引文件，不能存在的打开文件，存在的删除文件


        # #索引文件还不存在
        # for k in indexgroup:
        #     if not k in self._index_handles:
        #         try:
        #             self._index_handles[k] = open(k, 'r+')
        #         except IOError:
        #             createFile(k)
        #             self._index_handles[k] = open(k, 'r+')
        #
        #
        #
        # #磁盘中有索引文件，但是索引已经被删除
        # delete = []
        # for k in self._index_handles:
        #     if not k in indexgroup:
        #         delete.append(k)
        #
        # for k in delete:
        #     self._index_handles.pop(k)
        #     k.close()
        #     os.remove(k)



        # for k, v in self._index_handles.items():
        #     h = self._index_handles[k]
        #
        #     h.seek(0)
        #     Index.indexDump(v, h)
        #     h.flush()
        #     h.truncate()

import pprint

if __name__ == '__main__':

    a = Storage('test')

    d, i = a.read()

    print('db: ', d['testindex'])
    print('index:', i['testindex'])
    #
    #
    #
    #
    # #表级数据
    # data = {}
    # data['123'] = {'name': 'lifeng', 'age': 23}
    # # print(data)
    #
    #
    # index = SortedDict()
    # index['name'] = SortedDict()
    # Index.addindex(index, 'name', 'lifeng', '123')
    # # print(index)
    #
    #
    #
    # a.write(data, index)
    # # d, i = a.read()
    # # print('db: ', d)
    # # print('indexgroup:', i)

