import re
import sys
from urllib.error import HTTPError
from urllib.parse import urljoin
import threading

from document import WebDocument
from collection import Collection
# from src.document import WebDocument
# from src.collection import Collection
from bs4 import SoupStrainer

RE_LINKSPLIT = re.compile(r"[?#]")


class Webcrawler:
    def __init__(self, initial_links=list()):
        # self.links: dict with key each link that has been collected and value True if it has been visited by crawler
        self.links = dict()
        for l in initial_links:
            self.addlink(l)
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
        c=0
        depth = 0
        while not all(self.links.values()):
            print('depth: {}'.format(depth), file=sys.stderr)
            links_to_visit = [l for l,v in self.links.items() if not v]
            for link in links_to_visit:
                c += 1
                print("\rCount {}".format(c), end='', flush=True)
                try:
                    webdoc = WebDocument(link)
                    soup = webdoc.get_soup(parse_only=SoupStrainer("a"))
                    if soup:
                        for a in soup.find_all('a', href=True):
                            l = a['href']
                            if l.startswith('http'):
                                self.addlink(l)
                            else:
                                self.addlink(urljoin(webdoc.location, l))
                        if collection:
                            # collection.read_document(webdoc)
                            threading.Thread(target=collection.read_document(webdoc)).start()
                            print('Continue')
                        self.markvisited(link)
                    else:
                        self.markbad(link)
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
    collection = Collection()
    c = Webcrawler()
    # c.addlink('http://datalab.csd.auth.gr/%7Egounaris/courses/dwdm/index.html')
    c.addlink('http://snf-1510.ok-kno.grnetcloud.net/rick-and-morty-website/')
    c.crawl(collection=collection, maxdepth=-1)
    # c.crawl(maxdepth=1)
    #"""
    result = collection.processquery_vector("cut short", top=10)
    print("\nResults:")
    for d in result:
        print(d[0], d[1])
    print()
    #"""
