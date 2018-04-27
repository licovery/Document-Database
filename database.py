from .storage import Storage
from .query import Query
from .index import Index
from .LRU import LRU
from .middleware import CachingMiddleware

from sortedcontainers import SortedDict
from uuid import uuid1


# from LRU import LRU



class Document(object):
    def __init__(self, doc_id, element):
        self._doc_id = doc_id
        self._element = element

    @property
    def doc_id(self):
        return self._doc_id


    @property
    def element(self):
        return self._element


    @element.setter
    def element(self, element):
        self._element = element


    #连同id一起显示
    def __str__(self):
        doc = self._element
        doc['_id'] = self._doc_id
        return str(doc)

    __repr__ = __str__




class DocDb(object):

    #对数据库对象本身的访问就是default表
    def __init__(self, name, **kwargs):
        self._allcollections = {}
        storage = kwargs.pop('storage', Storage)
        self._storage = storage(name)


        #从硬盘读取数据建立数据库
        data, index = self._read()
        for coll in data:
            self.collection(coll)

        #建立默认表
        self._collection = self.collection()





    #集合存在则切换，集合不存在则创建，更新集合字典
    def collection(self, name='default'):
        if name in self._allcollections:
            return self._allcollections[name]

        else:
            coll = Collection(name, self)
            self._allcollections[name] = coll

            data, index = self._read()
            if name not in data:
                self._write({}, SortedDict(), name)
            return coll



    #需要析构collection吗？
    def dropCollection(self, name):

        #如何集合存在删除数据
        if name in self._allcollections:
            data, index = self._read()

            self._allcollections.pop(name)
            data.pop(name)

            #如果该集合存在索引
            if name in index:
                index.pop(name)


            self._write(data, index)




    #参数没有指定集合则读入整个数据库
    def _read(self, collection=None):
        if not collection:
            return self._storage.read()
        else:
            data , index = self._read()
            data = data[collection]
            index = index[collection]
            return data, index

    #如果不指定集合，则写整数据库
    def _write(self, data, index, collection=None):
        if not collection:
            self._storage.write(data, index)
        else:
            db_data, db_index = self._read()
            db_data[collection] = data
            db_index[collection] = index
            self._storage.write(db_data, db_index)


    #数据库的长度，也就是集合数
    def __len__(self):
        return len(self._allcollections)


    # 释放资源，持久化等等（未完成）
    def close(self):
        self._storage.close()


    #代理，把增删改查等等操作下放到表级操作
    def __getattr__(self, name):
        return getattr(self._collection, name)

class Collection(object):
    def __init__(self, name, db):
        self._name = name
        self._db = db
        self._cache = LRU()


    def _read(self):
        return self._db._read(self._name)


    def _write(self, data, index):
        self._db._write(data, index, self._name)


    #返回所有建立索引的字段
    def getIndexs(self):
        data, index = self._read()
        return [field for field in index]


    #某个字段是否有索引
    def hasIndex(self, field):
        data, index = self._read()
        return field in index

    #当开启写缓冲模式后，无法创建索引，bug!!!!!!!!!!!!!!
    def createIndex(self, field):
        data, index = self._read()

        if not field in index:
            index[field] = SortedDict()
            for docid, element in data.items():
                for field, value in element.items():
                    Index.addindex(index, field, value, docid)

        self._write(data, index)



    def dropIndex(self, field):
        data, index = self._read()

        if field in index:
            index.pop(field)

        self._write(data, index)


    #element是键值对组成的dict，就是文档
    def insert(self, element):
        data, index = self._read()
        self._cache.clear()

        docid = str(uuid1())
        data[docid] = element

        for field, value in element.items():
                Index.addindex(index, field, value, docid)

        self._write(data, index)


    #单字段查询可以使用索引
    def find(self, cond=None):
        data, index = self._read()

        if cond:
            if cond in self._cache:
                return self._cache.get(cond)
            else:
                res = self._findId(cond)
                ans = [Document(docid, data[docid]) for docid in res]
                self._cache.put(cond, ans)
                return ans
        else:
            return [Document(docid, data[docid]) for docid in data]


    def remove(self, cond):
        data, index = self._read()
        self._cache.clear()

        res = self._findId(cond)


        #对于这些id,从数据库删除，索引也删除
        for docid in res:
            for field, value in data[docid].items():
                Index.removeindex(index, field, value, docid)
            data.pop(docid)

        self._write(data, index)


    #附加，没得field-key加上，有的修改
    def update(self, element, cond):
        data, index = self._read()
        self._cache.clear()

        res = self._findId(cond)

        # 对于这些id,从数据库更新，索引也更新
        for docid in res:

            #某些要更新的字段
            for field, value in data[docid].items():
                if field in element:
                    Index.removeindex(index, field, value, docid)

            for field, value in element.items():
                Index.addindex(index, field, value, docid)

            data[docid].update(element)

        self._write(data, index)


    def writeBack(self, doc):
        data, index = self._read()
        self._cache.clear()

        for field, value in data[doc.doc_id].items():
            Index.removeindex(index, field, value, doc.doc_id)
            Index.addindex(index, field, value, doc.doc_id)
        data[doc.doc_id] = doc.element

        self._write(data, index)


    #调试用，输出index
    def all(self):
        data, index = self._read()
        return data, index


    def __len__(self):
        return len(self.all()[0])


    #清空集合内容，集合还存在
    def purge(self):
        self._write({}, SortedDict())
        self._cache.clear()


    #找到对应要求的ID(list)，根据情况使用索引或者不使用
    def _findId(self, cond):
        data, index = self._read()

        #单字段查询，并且该字段已经建立索引
        if isinstance(cond, Query) and cond.field in index:
            return cond.matchWithIndex(index[cond.field])

        else:
            return [docid for docid, element in data.items() if cond.match(element)]











    # 条件类型是Query,确保在索引里面了
    # def _findIdByIndex(self, cond):
    #     data, index = self._read()
    #     # 单值查询，可以索引
    #     res = []
    #
    #     if cond.eqvalue in index[cond.field]:
    #         res += list(index[cond.field][cond.eqvalue])
    #
    #     return res


    # #代码太短，可能没什么用
    # @staticmethod
    # def _adddocument(data, elemnet, docid):
    #     data[docid] = elemnet
    #
    # @staticmethod
    # def _removedocument(data, elemnet, docid):
    #     if docid in data:
    #         data.pop(elemnet)


if __name__ == '__main__':
    a = DocDb('neo')
    d , i = a._read()

    print(d['testindex'])
    print(i['testindex'])