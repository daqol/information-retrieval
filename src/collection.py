# -*- coding: utf-8 -*-

import heapq
import math
import operator
from collections import defaultdict
from pymongo import HASHED


from boolean_expression_parse import BooleanExpressionParser
from document import textpreprocess


READ_LIMIT_TO_WRITE_TO_MONGO = 100


class InvertedIndex(defaultdict):
    def __init__(self):
        super().__init__(dict)

    def add_document(self, d):
        """
        Adds a document in inverted index. It, also, calculates property L_d, which is the norm of vector of w
        as is in [chapter4-vector.pdf page 14].
        :param d: Document
        :return: L_d
        """
        l_d = 0
        for term, count in d.tokenize().items():
            # Document add
            self[term][d] = count
            # L_d calculation
            if count == 1:
                l_d += 1  # Faster for count=1, same result though
            else:
                l_d += (1 + math.log(count))**2

        return math.sqrt(l_d)


class Collection:
    def __init__(self, mongo_db=None, mongo_collections=None):
        """

        :param mongo_db: MongoClient object
        :param mongo_collections: dict with collections: 'invertedIndex' for inverted index collection (will contain
         term as key and document locations as value and 'documents' for documents collection (will contain document
         location as key and L_d as value)
        """
        self.index = InvertedIndex()  # inverted index
        self.documents = dict()  # dict with documents as keys and L_d as values
        self.mongo_db = mongo_db
        self.mongo_collections = mongo_collections

    def flush_to_mongo(self):
        if self.index and self.documents:
            # Write to mongo
            for term, docs in self.index.items():
                mdocs = [{'doc': doc.location, 'count': count} for doc, count in docs.items()]
                self.mongo_db[self.mongo_collections['invertedIndex']].update({'term': term}, {"$push": {"docs": {"$each": mdocs}}}, upsert=True)
            mdocs = [{'doc': doc.location, 'L_d': L_d} for doc, L_d in self.documents.items()]
            self.mongo_db[self.mongo_collections['documents']].insert_many(mdocs)

            # Clear memory
            self.index.clear()
            self.documents.clear()

    def create_mongo_indexes(self):
        self.mongo_db[self.mongo_collections['invertedIndex']].create_index([('term', HASHED)])
        self.mongo_db[self.mongo_collections['documents']].create_index([('doc', HASHED)])

    def get_documents_count(self):
        return self.mongo_db[self.mongo_collections['documents']].count()

    def get_index_count(self):
        return self.mongo_db[self.mongo_collections['invertedIndex']].count()

    def get_documents_for_term(self, term):
        ans = self.mongo_db[self.mongo_collections['invertedIndex']].find_one({'term': term}, {'docs': 1})
        if ans:
            return ans['docs']
        else:  # Check if term is in our collection
            raise Exception("Term '" + term + "' does not exist in our inverted index.")

    def get_only_documents_for_term(self, term):
        ans = self.mongo_db[self.mongo_collections['invertedIndex']].find_one({'term': term}, {'docs': 1})
        return set([doc_entry['doc'] for doc_entry in ans['docs']]) if ans else set()

    def get_documents_not_in(self, other_doc_set):
        ans = self.mongo_db[self.mongo_collections['documents']].find({'doc': {"$nin": list(other_doc_set)}}, {'doc': 1})
        return set([doc_entry['doc'] for doc_entry in ans]) if ans else set()

    def get_document_L_d(self, doc: str):
        ans = self.mongo_db[self.mongo_collections['documents']].find_one({'doc': doc}, {'L_d': 1})
        if ans:
            return ans['L_d']
        else:  # Check if doc is in our collection
            raise Exception("Document '" + doc + "' does not exist in our collection.")

    def read_document(self, d):
        """
        Reads a document add adds its terms in inverted index. It is also adds documents' L_d in self.documents set

        :param d:
        :return:
        """
        if d not in self.documents:
            self.documents[d] = self.index.add_document(d)

            if len(self.index) >= READ_LIMIT_TO_WRITE_TO_MONGO:
                self.flush_to_mongo()

    def processquery_boolean(self, q):
        """
        Processes a query with the boolean model.
        :param q: query in boolean expression format
        :return: Documents that satisfy the query
        """
        def term_documents(term):
            """ Returns documents in this index that contain term """
            return self.get_only_documents_for_term(term)

        def rest_documents(documents_set):
            """ Returns set difference between this index's documents and documents_set"""
            return self.get_documents_not_in(documents_set)

        # get a BooleanExpressionParser, which will evaluate the query
        bparser = BooleanExpressionParser(term_documents, rest_documents)

        # process query text the same way as a document (do the same text preprocessing)
        newq = ' '.join(textpreprocess(q))

        return bparser.eval_query(newq)

    def processquery_vector(self, q, above=0.2, top=-1):
        """
        Processes a query with the vector model. Based on [chapter4-vector.pdf page 14]
        :param q: query. Any sentence
        :param above: lowest limit in similarity of documet and query. Defaults to 0.2
        :param top: Returns top x documents if is set. Defaults to unlimited (-1)
        :return: Documents that satisfy the query
        """
        def tf_t_d(f_t_d):
            return 1+math.log(f_t_d) if f_t_d > 1 else 1

        q_tokens = textpreprocess(q)

        # S for Sums. Dict with key a document and value similarity computation
        S = defaultdict(float)

        # N: count of collection documents
        N = self.get_documents_count()

        for term in q_tokens:
            docs_for_term = self.get_documents_for_term(term)
            # n_t: Count of documents that contain term
            n_t = len(docs_for_term)

            # idf_t: inverse frequency of documents for term
            idf_t = math.log(1 + N / n_t)

            for doc_entry in docs_for_term:
                S[doc_entry['doc']] += tf_t_d(doc_entry['count']) * idf_t

        for d in S.keys():
            S[d] /= self.get_document_L_d(d)

        # Keep only top k and with similarity above lower limit
        S_passed = [(k, v) for k, v in S.items() if v >= above] if above > 0 else S.items()
        return sorted(S_passed, key=operator.itemgetter(1), reverse=True) if top < 0 else heapq.nlargest(top, S_passed, key=operator.itemgetter(1))

