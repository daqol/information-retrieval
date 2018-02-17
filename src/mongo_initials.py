# -*- coding: utf-8 -*-

from pymongo import MongoClient, HASHED


# let
session_id = '3842u34h23480'

mdb = MongoClient()['inforet']
mcolls = {'invertedIndex': 'index' + session_id, 'documents': 'docs' + session_id}

# mdb[mcolls['invertedIndex']].create_index([('term', HASHED)])
# mdb[mcolls['documents']].create_index([('term', HASHED)])
