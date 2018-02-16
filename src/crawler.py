import re
import sys
from urllib.error import HTTPError
from urllib.parse import urljoin

from src.mongo_initials import *

from document import WebDocument
from collection import Collection
from bs4 import SoupStrainer

RE_LINKSPLIT = re.compile(r"[?#]")


class Webcrawler:
    def __init__(self, initial_links=list()):
        # self.links: dict with key each link that has been collected and value True if it has been visited by crawler
        self.links = dict()
        for l in initial_links:
            self.addlink(l)
        # bad_lnks: set with links that are not good for indexing and/or link scanning for any reason and they must not be included in any activity
        self.badlinks = set()

    def markvisited(self, link):
        self.links[link] = True

    def markbad(self, link):
        del self.links[link]
        self.badlinks.add(link)

    def addlink(self, link):
        if link not in self.links and link not in self.badlinks:
            self.links[RE_LINKSPLIT.split(link, 1)[0]] = False

    def addlinks_and_crawl(self, links=list()):
        for l in links:
            self.addlink(l)
        self.crawl()

    def crawl(self, maxdepth=-1, collection=None):
        """
        Crawls the Web starting from a list with initial links (self.links).
        :param maxdepth: This is the depth that crawler will reach. Initial links are in depth 0. Links of initial links
         are in depth 1 and etc. Defaults to unlimited (-1).
        :param collection: Collection object. If it is given it will call collection.read_document function for every
         web document that is read.
        :return:
        """
        c = 0  # DEBUG: counter
        depth = 0
        # Loop till all documents in self.links are read
        while not all(self.links.values()) and c<50:
            print('depth: {}'.format(depth), file=sys.stderr)  # DEBUG
            # Get links which have not been visited
            links_to_visit = [l for l, v in self.links.items() if not v]
            for link in links_to_visit:
                c += 1
                if c>50:
                    break
                print("\rCount {}".format(c), end='', flush=True)  # DEBUG
                try:
                    # Create web doc
                    webdoc = WebDocument(link)
                    # Get a parser (soup) for doc
                    soup = webdoc.get_soup(parse_only=SoupStrainer("a"))
                    if soup:
                        # For each <a> in doc add link to self.links
                        for a in soup.find_all('a', href=True):
                            l = a['href']
                            if l.startswith('http'):  # absolute links
                                self.addlink(l)
                            else:                     # relative links
                                self.addlink(urljoin(webdoc.location, l))
                        if collection:
                            collection.read_document(webdoc)
                            # threading.Thread(target=collection.read_document(webdoc)).start()
                        self.markvisited(link)
                    else:  # if soup does not exist something's wrong with this doc
                        self.markbad(link)
                # if we get an HTTP error or anything goes wrong, mark bad the document and go on
                except HTTPError as e:
                    print("{} returned code error {}".format(link, e.code), file=sys.stderr)
                    self.markbad(link)
                except:
                    print("Unknown error for {}".format(link), file=sys.stderr)
                    self.markbad(link)

            if depth-maxdepth == 0:
                break
            depth += 1

    def getlinks(self):
        return self.links.keys()


if __name__ == '__main__':
    # mclient = MongoClient()['inforet']
    # mcolls = {'invertedIndex': 'invertedIndex', 'documents': 'documents'}

    collection = Collection(mdb, mcolls)
    c = Webcrawler()
    print(collection.__dict__)
    print(c.__dict__)
    # c.addlink('http://datalab.csd.auth.gr/%7Egounaris/courses/dwdm/index.html')
    c.addlink('http://snf-1510.ok-kno.grnetcloud.net/rick-and-morty-website/')
    c.crawl(collection=collection, maxdepth=-1)
    collection.flush_to_mongo()
    mdb[mcolls['invertedIndex']].create_index([('term', HASHED)], background=True)
    mdb[mcolls['documents']].create_index([('term', HASHED)], background=True)
    # c.crawl(maxdepth=1)
    #"""
    result = collection.processquery_vector("cut short", top=10)
    print("\nResults:")
    for d in result:
        print(d[0], d[1])
    print()
    #"""
