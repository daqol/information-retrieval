from pyparsing import infixNotation, opAssoc, Word, alphanums


class BooleanExpressionParser:
    """
    Boolean expression parser
    """

    def __init__(self, evalfn, NOTevalfunc):
        """
        :param evalfn: Function that will be called with argument a boolean operand for each boolean operand
                        Example: In expression A: a and not b, there will be 2 calls of evalfunc, evalfunc(a) and
                        evalfunc(b) in order to convert a,b in appropriate data.
        :param NOTevalfunc: Function that will be called to get result of NOT operator
                        Example: In expression A: a and not b, there will be a call to NOTevalfunc passing as argument
                        the result of evalfunc(b). It can be seen as NOTevalfunc(evalfunc(b)).
        """

        # operand type
        # TODO check for greeks and so
        self.boolOperand = Word(alphanums+'_-')
        self.boolOperand.setParseAction(lambda t: evalfn(t[0]))

        # expression grammar
        self.boolExpr = infixNotation(self.boolOperand,
                                      [
                                          ("not", 1, opAssoc.RIGHT, lambda t: NOTevalfunc(t[0][1])),
                                          ("and", 2, opAssoc.LEFT, lambda t: set(t[0][0]).intersection(*t[0][2::2])),
                                          ("or", 2, opAssoc.LEFT, lambda t: set(t[0][0]).union(*t[0][2::2]))
                                      ])

    def eval_query(self, q):
        """
        Evals query and returns set of documents that satisfy it
        :param q: boolean expression
        :return: result of boolean expression
        """
        return self.boolExpr.parseString(q)[0]

