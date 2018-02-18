#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
from collections import Set, Mapping, deque
from numbers import Number

from collection import Collection
from crawler import Webcrawler
from document import LocalDocument
from mongo_initials import *
from pymongo import MongoClient

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


def get_Collection_from_mongo_initial(args):
    mongodb = MongoClient(host=args.mongo_host, port=args.mongo_port)[args.mongo_database]
    mongo_colls = {'invertedIndex': args.mongo_collection_index, 'documents': args.mongo_collection_docs}
    return Collection(mongodb, mongo_colls)


def process_search(args):
    collection = get_Collection_from_mongo_initial(args)
    result = collection.processquery_boolean(args.query) if args.model == 'boolean' else \
        collection.processquery_vector(args.query, above=args.above, top=args.top)
    # print("\nResults:")
    for d in result:
        print("{},{:.2f}".format(d[0], d[1]))


def process_index_local(args):
    collection = get_Collection_from_mongo_initial(args)
    for (dirname, _, filenames) in os.walk(args.directory):
        for filename in filenames:
            d = LocalDocument(os.path.join(dirname, filename))
            collection.read_document(d)
    collection.flush_to_mongo()

    if args.create_mongo_indexes:
        collection.create_mongo_indexes()


def process_web_crawl(args):
    collection = get_Collection_from_mongo_initial(args)
    crawler = Webcrawler(args.seed)
    crawler.crawl(maxdepth=args.max_depth, collection=collection)
    collection.flush_to_mongo()

    if args.create_mongo_indexes:
        collection.create_mongo_indexes()


if __name__ == '__main__':
    # Arguments parsing
    parser = argparse.ArgumentParser(description="Search Engine.")
    subparsers = parser.add_subparsers(title="Available commands", help="Commands Description")

    parser_search = subparsers.add_parser("search", help="Search a query")
    parser_search.add_argument('-m', '--model', choices=["boolean", "vector"], required=True, help="Model to use for quering")
    parser_search.add_argument('--above', type=float, default=0.2, help="Lower limit in document-query similarity. Default: 0.2. Valid only for vector model")
    parser_search.add_argument('--top', type=int, default=-1, help="Top k documents based on document-query similarity. Default: Unlimited (-1). Valid only for vector model")
    parser_search.add_argument('query', help="Query. Logic expression with keywords and {AND, NOT, OR} for boolean model, anything for vector model.")
    parser_search.set_defaults(func=process_search)

    parser_index_local = subparsers.add_parser("index-local", help="Index local documents")
    parser_index_local.add_argument('-D', '--directory', default="documents", help="Directory which contains documents to index. Default: documents")
    parser_index_local.set_defaults(func=process_index_local)

    parser_web_crawl = subparsers.add_parser("web-crawl", help="Crawl the Web. This crawls the web and collects links from sites and indexes every site that has been visited")
    parser_web_crawl.add_argument('-s', '--seed', required=True, nargs='+', help="Initial link(s) for crawl beginning")
    parser_web_crawl.add_argument('-m', '--max-depth', default=-1, help="This is the depth that crawler will reach. Initial links are in depth 0. Links of initial links are in depth 1 and etc. Default: Unlimited (-1)")
    parser_web_crawl.set_defaults(func=process_web_crawl)

    parser.add_argument('-H', '--mongo-host', default="localhost", help="MongoDB host. Default: localhost")
    parser.add_argument('-p', '--mongo-port', type=int, default=27017, help="MongoDB port. Default: 27017")
    parser.add_argument('-d', '--mongo-database', default="inforet", help="MongoDB database. Default: inforet")
    parser.add_argument('-i', '--mongo-collection-index', required=True, help="MongoDB collection for inverted index")
    parser.add_argument('-l', '--mongo-collection-docs', required=True, help="MongoDB collection for documents' L_d")
    parser.add_argument('-I', '--create-mongo-indexes', action='store_true', help="If is set a mongoDB index will be created for each collection that will be created after the read of documents. Valid only for commands: index-local and web-crawl.")

    args = parser.parse_args()
    args.func(args)

    '''
    ### Queries ###

    collection = Collection(mdb, mcolls)
    directory = 'documents'

    # Read documents
    for (dirname, _, filenames) in os.walk(directory):
        for filename in filenames:
            d = LocalDocument(os.path.join(dirname, filename))
            collection.read_document(d)

    d = WebDocument('http://www.csd.auth.gr/el/')
    collection.read_document(d)

    collection.flush_to_mongo()
    print('Let\'s go team!')

    # Make query
    q = "katrina and hurricane"
    k = 4
    # result = collection.processquery_vector(q, above=0)
    result = collection.processquery_boolean(q)
    print("\nResults:")
    for d in result:
        print(d)
        # print("{} with: {}".format(d[0], d[1]))
    #print()
    #print(getsize(index.index))
    '''
