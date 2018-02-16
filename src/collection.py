import heapq
import math
import operator
from collections import defaultdict


from src.boolean_expression_parse import BooleanExpressionParser
from src.document import textpreprocess


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
    def __init__(self):
        self.index = InvertedIndex()  # inverted index
        self.documents = dict()  # dict with documents as keys and L_d as values

    def read_document(self, d):
        """
        Reads a document add adds its terms in inverted index. It is also adds documents' L_d in self.documents set

        :param d:
        :return:
        """
        if d not in self.documents:
            self.documents[d] = self.index.add_document(d)


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
            return self.documents.keys() - documents_set

        # get a BooleanExpressionParser, which will evaluate the query
        bparser = BooleanExpressionParser(term_documents, rest_documents)

        # process query text the same way as a document (do the same text preprocessing)
        newq = ' '.join(textpreprocess(q))

        return bparser.eval_query(newq)

    def processquery_vector(self, q, top=10):

        q_tokens = textpreprocess(q)

        # Check if all terms of query are in our collection
        for token in q_tokens:
            if token not in self.index:
                raise Exception(token + " does not exist in our collection.")

        S = defaultdict(float)

        for term in q_tokens:
            idf_t = math.log(1 + len(self.documents) / len(self.index[term]))

            for d, v in self.index[term].items():
                S[d] += v * idf_t

        for d in S.keys():
            S[d] /= self.documents[d]

        return heapq.nlargest(top, S.items(), key=operator.itemgetter(1))

