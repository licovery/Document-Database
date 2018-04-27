from sortedcontainers import SortedDict

#复合类型
class AndOrMixin(object):
    def __or__(self, other):
        return QueryOr(self, other)

    def __and__(self, other):
        return QueryAnd(self, other)



#单字段查询
class Query(AndOrMixin):
    def __init__(self, field):
        self._field = field
        self._repr = 'has field \'%s\'' % self._field

    #只读属性
    @property
    def field(self):
        return self._field



    # <
    def __lt__(self, other):
        self._lt_value = other
        self._repr = '\'%s\' < %s' % (self._field, other)
        return self

    # <=
    def __le__(self, other):
        self._le_value = other
        self._repr = '\'%s\' <= %s' % (self._field, other)
        return self

    # ==
    def __eq__(self, other):
        if isinstance(other, Query):
            return repr(self) == repr(other)
        else:
            self._eq_value = other
            self._repr = '\'%s\' == %s' % (self._field, other)
            return self

    # !=
    def __ne__(self, other):
        self._ne_value = other
        self._repr = '\'%s\' != %s' % (self._field, other)
        return self

    # >=
    def __ge__(self, other):
        self._ge_value = other
        self._repr = '\'%s\' >= %s' % (self._field, other)
        return self

    # >
    def __gt__(self, other):
        self._gt_value = other
        self._repr = '\'%s\' > %s' % (self._field, other)
        return self

    def __repr__(self):
        return self._repr

    __str__ =  __repr__


    def __hash__(self):
        return hash(repr(self))

    def match(self, doc):
        if self._field not in doc:
            return False

        try:
            return doc[self._field] < self._lt_value
        except AttributeError:
            pass

        try:
            return doc[self._field] <= self._le_value
        except AttributeError:
            pass

        try:
            return doc[self._field] == self._eq_value
        except AttributeError:
            pass

        try:
            return doc[self._field] != self._ne_value
        except AttributeError:
            pass

        try:
            return doc[self._field] >= self._ge_value
        except AttributeError:
            pass

        try:
            return doc[self._field] > self._gt_value
        except AttributeError:
            pass

        #不用值查询，直接字段查询
        return self._field in doc


    #index是某个字段的索引(该字段已经在索引中，直接查值就好）
    def matchWithIndex(self, index):

        res = []

        try: #<
            pos = index.bisect_left(self._lt_value)
            for value in index.iloc[:pos]:
                res += list(index[value])
            return res
        except AttributeError:
            pass

        try: #>=
            pos = index.bisect_left(self._ge_value)
            for value in index.iloc[pos:]:
                res += list(index[value])
            return res
        except AttributeError:
            pass

        try: #<=
            pos = index.bisect_right(self._le_value)
            for value in index.iloc[:pos]:
                res += list(index[value])
            return res
        except AttributeError:
            pass

        try:  # >
            pos = index.bisect_right(self._gt_value)
            for value in index.iloc[pos:]:
                res += list(index[value])
            return res
        except AttributeError:
            pass

        try:  # ==
            if self._eq_value in index:
                res += list(index[self._eq_value])
            return res
        except AttributeError:
            pass

        try:  # !=
            for value, doc_id in index.items():
                if value != self._ne_value:
                    res += list(doc_id)
            return res
        except AttributeError:
            pass

        #返回所有该字段的值
        for doc_id in index.values():
            res += list(doc_id)
        return res


class QueryAnd(AndOrMixin):
    def __init__(self, query1, query2):
        self._query1 = query1
        self._query2 = query2

    def __repr__(self):
        return '(%s and %s)' % (repr(self._query1), repr(self._query2))

    __str__ = __repr__


    #重写这两个特殊方法使得该类的示例可以作为key放进dict里面

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

    def match(self, doc):
        return self._query1.match(doc) and self._query2.match(doc)



class QueryOr(AndOrMixin):
    def __init__(self, query1, query2):
        self._query1 = query1
        self._query2 = query2


    def __repr__(self):
        return '(%s or %s)' % (repr(self._query1), repr(self._query2))

    __str__ = __repr__


    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

    def match(self, doc):
        return self._query1.match(doc) or self._query2.match(doc)


where = Query