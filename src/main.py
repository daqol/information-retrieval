# -*- coding: utf-8 -*-

import os
import sys
import json
import pymongo
from pymongo import MongoClient
from collections import Set, Mapping, deque
from numbers import Number


from src.collection import Collection
from src.document import LocalDocument, WebDocument

zero_depth_bases = (str, bytes, Number, range, bytearray)
iteritems = 'items'


def getsize(obj_0):
    """Recursively iterate to sum size of object & members."""
    def inner(obj, _seen_ids=set()):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0
        _seen_ids.add(obj_id)
        size = sys.getsizeof(obj)
        if isinstance(obj, zero_depth_bases):
            pass # bypass remaining control flow and return
        elif isinstance(obj, (tuple, list, Set, deque)):
            size += sum(inner(i) for i in obj)
        elif isinstance(obj, Mapping) or hasattr(obj, iteritems):
            size += sum(inner(k) + inner(v) for k, v in getattr(obj, iteritems)())
        # Check for custom object instances - may subclass above too
        if hasattr(obj, '__dict__'):
            size += inner(vars(obj))
        if hasattr(obj, '__slots__'): # can have __slots__ with __dict__
            size += sum(inner(getattr(obj, s)) for s in obj.__slots__ if hasattr(obj, s))
        return size
    return inner(obj_0)


if __name__ == '__main__':
    ### Queries ###

    index = Collection()
    directory = 'documents'

    # Read documents
    for (dirname, _, filenames) in os.walk(directory):
        for filename in filenames:
            d = LocalDocument(os.path.join(dirname, filename))
            index.read_document(d)

    d = WebDocument('http://www.csd.auth.gr/el/')
    index.read_document(d)
    print('Let\'s go team!')

    # Make query
    q = "tropical"
    k = 4
    result = index.processquery_vector(q, k)
    print("\nResults:")
    for d in result:
        #print(d)
        print(d[0] + " with: " + d[1])
    #print()
    #print(getsize(index.index))

