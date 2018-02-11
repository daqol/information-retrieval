import heapq
from collections import defaultdict
import operator

import math

from src.boolean_expression_parse import BooleanExpressionParser
from src.document import textpreprocess


class InvertedIndex:

    def __init__(self):
        self.index = defaultdict(dict)
        self.documents = set()

    def read_document(self, d):
        """
        Reads a document add adds its terms in
        :param d:
        :return:
        """
        self.documents.add(d)

        for term, count in d.tokenize().items():
            self.index[term][d] = count

    def processquery_boolean(self, q):
        """
        Processes a query with the boolean model.
        :param q: query in boolean expression format
        :return: Documents that satisfy the query
        """

        def term_documents(term):
            """ Returns documents in this index that contain term"""
            return self.index.get(term, dict()).keys()

        def rest_documents(documents_set):
            """ Returns set difference between this index's documents and documents_set"""
            return self.documents - documents_set

        bparser = BooleanExpressionParser(term_documents, rest_documents)

        # process query text the same way as a document
        newq = ' '.join(textpreprocess(q))

        return bparser.eval_query(newq)

    def processquery_vector(self, q):

        q_tokens = textpreprocess(q)

        # Check if all terms of query are in our collection
        for token in q_tokens:
            if token not in self.index:
                raise Exception(token + " does not exist in our collection.")

        # idf = [math.log(1 + len(self.index) / len(self.index[token])) for token in q_tokens]

        S = defaultdict(float)

        for term in q_tokens:
            idf_t = math.log(1 + len(self.documents) / len(self.index[term]))

            for d, v in self.index[term].items():
                S[d] += v * idf_t

        for d, v in S.items():
            S[d] /= d.L_d

        return heapq.nlargest(3, S.items(), key=operator.itemgetter(1))

