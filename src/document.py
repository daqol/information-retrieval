import io
import math
import re
import urllib.request
from abc import abstractmethod
from collections import Counter

import chardet
# from html2text import html2text

from bs4 import BeautifulSoup, SoupStrainer
from stemming.porter2 import stem


RE_LINKS = re.compile(r'https?\S+')
RE_NONALPHABETIC = re.compile(r'[^ \w-]+')

"""
RE_CHARSET = re.compile(r"<head>.*?charset=\"?[\w-]+")
def get_unicode_text(request):

    bytetext = request.read()

    # get encoding. First choice <meta> tag charset field in html. Second choice server response header. Fallback: utf-8
    # m = RE_CHARSET.search(text)
    encoding = request.headers.get_content_charset()
    if not encoding:
        encoding = chardet.detect(bytetext)['encoding']
    # encoding = chardet.detect(bytetext)['encoding']

    # decode response text based on right encoding and return unicode string
    decodedtext = bytetext.decode(encoding)
    return decodedtext # if encoding == 'utf-8' else decodedtext.encode('utf-8')
"""

stopwords = {"i","me","my","myself","we","our","ours","ourselves","you",
                 "your","yours","yourself","yourselves","he","him","his","himself",
                 "she","her","hers","herself","it","its","itself","they","them","their",
                 "theirs","themselves","what","which","who","whom","this","that","these",
                 "those","am","is","are","was","were","be","been","being","have","has",
                 "had","having","do","does","did","doing","a","an","the","but",
                 "if","because","as","until","while","of","at","by","for","with",
                 "about","against","between","into","through","during","before","after",
                 "above","below","to","from","up","down","in","out","on","off","over",
                 "under","again","further","then","once","here","there","when","where",
                 "why","how","all","any","both","each","few","more","most","other","some",
                 "such","no","nor","only","own","same","so","than","too","very","s",
                 "t","can","will","just","don","should","now"}  # {and, or, not} deleted


def textpreprocess(text):
    """
    Text is preprocessed in the following way:
            1. links removal
            2. Non-alphabetic removal. Every character that is not a letter or a number or '_' or '-' is removed
            3. stopwords removal
            4. words stemming
    :param text:
    :return: tokens of text after processing
    """

    # clear links and nonalphabetics from text
    text = RE_LINKS.sub(' ', text)
    text = RE_NONALPHABETIC.sub(' ', text)

    return [stem(word.lower()) for word in text.split() if word.lower() not in stopwords]


class Document:
    """
    Document Base Class
    """

    def __int__(self, location):
        self.location = location
        self.L_d = 0

    def __str__(self):
        return self.location

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.location == other.location
        return NotImplemented

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

    @abstractmethod
    def open(self):
        """
        Opens a stream to Document's location
        :return: stream to Document
        """
        pass

    @abstractmethod
    def read(self) -> str:
        """
        Reads Document.
        :return: unicode string with Document's text
        """
        pass

    def tokenize(self):
        """
        Tokenizes Document text. Text is preprocessed by :func:`textpreprocess`. After the call of this method object
        will also has property L_d, which is the norm of vector of w as is in [chapter4-vector.pdf page 14].
        :return: dict with each term in document as a key and its number of appearances as value
        """
        ans = Counter(textpreprocess(self.read()))

        l_d = 0
        for v in ans.values():
            if v == 1:
                l_d += 1
            else:
                l_d += (1 + math.log(v))**2
        self.L_d = math.sqrt(l_d)

        return ans


class LocalDocument(Document):
    """
    Document that is stored locally as a file
    """
    def __init__(self, location):
        super().__int__(location)

    def __str__(self):
        return self.location.rsplit('/', 1)[1]

    def open(self):
        return io.open(self.location, "r", encoding="utf-8")

    def read(self):
        with self.open() as f:
            text = f.read()
        return text


class WebDocument(Document):
    """
    HTML Document in the Web
    """
    def __init__(self, location):
        super().__int__(location)

    def open(self):
        req = urllib.request.urlopen(self.location)
        self.location = req.geturl()  # in case we were redirected
        return req

    def get_soup(self, parse_only=None):
        req = self.open()
        return BeautifulSoup(req, "lxml", parse_only=parse_only) if req.headers.get_content_type() == 'text/html' else None

    def read(self):
        soup = self.get_soup()
        return soup.get_text() if soup else None
        # return html2text(get_unicode_text(self.open()))


"""
    def persist(self):
        # Enables cache. Creates and stores BeatifulSoup object if it's not already cached 
        self.soup = self.get_soup()

    def unpersist(self):
        # Disables cache and deletes stored text. 
        del self.soup
"""
