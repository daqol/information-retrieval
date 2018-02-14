from urllib.error import HTTPError
from urllib.request import urlopen
from urllib.parse import urljoin
import sys
import re
import chardet


RE_LINK = re.compile(r"<a\s+href=\"([^ \s@]+)\"")
RE_CHARSET = re.compile(r"<head>.*?charset=\"?[\w-]+")

def get_unicode_text(request):

    bytetext = request.read()

    # get encoding. First choice <meta> tag charset field in html. Second choice server response header. Fallback: utf-8
    #m = RE_CHARSET.search(text)
    #encoding = m.group(0) if m else request.headers.get_content_charset('utf-8')
    encoding = chardet.detect(bytetext)['encoding']

    # decode response text based on right encoding and return unicode string
    decodedtext = bytetext.decode(encoding)
    return decodedtext #if encoding == 'utf-8' else decodedtext.encode('utf-8')


class Webcrawler:
    def __init__(self, initial_links=list()):
        # self.links: dict with key each link that has been collected and value True if it has been visited by crawler
        self.links = dict.fromkeys(initial_links, False)
        self.badlinks = set()

    def markvisited(self, link):
        self.links[link] = True

    def markbad(self, link):
        del self.links[link]
        self.badlinks.add(link)

    def addlink(self, link):
        if link not in self.links and link not in self.badlinks:
            self.links[link] = False

    def addlinks_and_crawl(self, links=list()):
        for l in links:
            self.addlink(l)
        self.crawl()

    def crawl(self):
        c=0
        depth = 0
        while not all(self.links.values()):
            print('depth: {}'.format(depth))
            links_to_visit = [l for l,v in self.links.items() if not v]
            for link in links_to_visit:
                c += 1
                print("\rCount {}".format(c), end='', flush=True)
                try:
                    r = urlopen(link)
                    if r.headers.get_content_type() == 'text/html':
                        text = get_unicode_text(r)
                        for l in RE_LINK.findall(text):
                            if l.startswith('http'):
                                self.addlink(l)
                            else:
                                self.addlink(urljoin(r.geturl(), l))
                        self.markvisited(link)
                    else:
                        self.markbad(link)
                except HTTPError as e:
                    print("{} returned code error {}".format(link, e.code), file=sys.stderr)
                    self.markbad(link)
                except:
                    self.markbad(link)
            depth += 1

    def getlinks(self):
        return self.links.keys()


if __name__ == '__main__':
    c = Webcrawler()
    c.addlink('http://datalab.csd.auth.gr/%7Egounaris/courses/dwdm/index.html')
    # c.addlink('http://quotes.toscrape.com')
    c.crawl()
    print(c.getlinks())

