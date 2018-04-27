from sortedcontainers import SortedDict
import pprint
import pickle

#提供序列化方法


#表级别的索引
class Index(object):

    # def __init__(self, db):
    #     self._db = db
    #     self._index = SortedDict()
    #
    # @property
    # def collection(self):
    #     return self._db.collection()
    #
    # #是否key的索引
    # def hasIndex(self, field):
    #     return field in self._index
    #
    #
    #
    # #以下函数默认field字段已经建立索引
    # #参数是一个键值对 field value
    # def insert(self, field, value, docid):
    #     if not value in self._index[field]:
    #         self._index[field][value] = set(docid)
    #     else:
    #         self._index[field][value].add(docid)
    #
    #
    # #删除一个索引结点
    # def remove(self, field, value, docid):
    #     if value in self._index[field] and docid in self._index[field][value]:
    #         self._index[field][value].reomve(docid)
    #         #已经没有可索引的docid了
    #         if len(self._index[field][value]) == 0:
    #             self._index[field].pop(value)
    #
    #
    #
    # #修改某个值对应的索引
    # def update(self, field, value1, value2, docid):
    #     self.remove(field, value1, docid)
    #     self.insert(field, value2, docid)
    #     pass
    #     #未完善
    #
    #
    #
    # def find(self, field, value):
    #     res = []
    #     if value in self._index[field]:
    #         res += list(self._index[field][value])
    #     return res
    #
    #
    #
    # #重复建立问题
    # def createIndex(self, collection ,field):
    #     if not self.hasIndex(field):
    #         self._index[field] = SortedDict()
    #
    #         docs = self._collection.find()
    #         for d in docs:
    #             self.insert(field, d[field], d['_id'])
    #
    #
    # #存在才删除
    # def dropIndex(self, field):
    #     if self.hasIndex(field):
    #         self._index.pop(field)
    #
    #
    # def show(self):
    #     pprint.pprint(self._index)


    #把value-id键值对添加到field索引中,如果字段没有索引则跳过
    @staticmethod
    def addindex(index, field, value, doc_id):
        if field in index:#该字段确实有索引
            if not value in index[field]:#该值还没建立过索引
                index[field][value] = set()
            index[field][value].add(doc_id)




    # 把value-id键值对从field索引中删除,如果字段没有索引则跳过
    @staticmethod
    def removeindex(index, field, value, doc_id):
        if field in index:#该字段有索引，那么值和id肯定有，直接删除即可
            index[field][value].remove(doc_id)
            #清理多余结点
            if len(index[field][value]) == 0:
                index[field].pop(value)



    # @staticmethod
    # def load(handle):
    #     return pickle.load(handle)
    #
    #
    # @staticmethod
    # def dump(index, handle):
    #     pickle.dump(index, handle)




