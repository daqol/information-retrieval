# -*- coding: utf-8 -*-

import os
import sys
import json
import pymongo
from pymongo import MongoClient
from collections import Set, Mapping, deque
from numbers import Number


from src.collection import Collection
from src.document import LocalDocument

zero_depth_bases = (str, bytes, Number, range, bytearray)
iteritems = 'items'


def getsize(obj_0):
    """Recursively iterate to sum size of object & members."""
    def inner(obj, _seen_ids = set()):
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

    index = Collection()

    directory = 'documents'

    for (dirname, _, filenames) in os.walk(directory):
        for filename in filenames:
            d = LocalDocument(os.path.join(dirname, filename))
            index.read_document(d)


    # d = WebDocument('web', 'http://www.csd.auth.gr/el/')
    # text = html2text.html2text(d.open().read().decode('utf-8'))
    # index.read_document(d)
    print('Let\'s go team!')
    #print(index.index.items())



    ###MongoDB###

    client = MongoClient('mongodb://localhost:27017/')
    db = client.test_irproject
    collection = db.inverted_index_collection
    #collection.insert(index.index)


    #print(index.documents)

    for key, value in index.index.items():
        for key2, value2 in value.items():
            post = {key: {str(key2).replace(".","-") : value2 }}
            post_id = collection.insert_one(post).inserted_id
            print(post)
            #if str(key2) == 'd1.txt':
            #    print("Yeap")

        

    a = list(index.index.items())
    print(a[0][1].keys())

    #for i in a:
    #    print(i)


    # attempt to convert document.LocalDocument to str representation
    #for value in index.index.values():
    #    for key, val in value.items():
    #        print(key)
    #print(index.index.items())


    #with open('data.json', 'w') as fp:
    #    json.dump(a, fp)



    ### Queries ###

    #result = index.processquery_vector("κομήτης Χάλλεϋ")
    #result = index.processquery_boolean("Tropical AND (Katarina OR Precursor)")
    q = "tropical"
    k=3
    result = index.processquery_vector(q,k)
    print("\nResults:")
    for d in result:
        print(str(d[0]) + " with: " + str(d[1]))
        print(type(d[0]))
    print()
    print(getsize(index.index))

