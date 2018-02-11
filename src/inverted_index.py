from collections import defaultdict

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


