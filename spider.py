from urllib.request import urlopen
from link_finder import LinkFinder
from domain import *
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from datetime import datetime


class Spider:

    base_url = ''
    domain_name = ''
    queue = set()
    crawled = set()
    host = ''
    awsauth = ''

    def __init__(self, base_url, domain_name):

        Spider.base_url = base_url
        Spider.domain_name = domain_name
        Spider.host = 'search-mysearchengine-7canadtuf2dlzoj5bvjeqwqufe.us-east-1.es.amazonaws.com'
        Spider.awsauth = AWS4Auth('AKIAJL3TXNQWGSIHZ5LQ', '+OP3FeY1vIr4S6TDG6yZMSQRYL8oaxJyu4pSYFSQ', 'us-east-1', 'es')
        self.initialize()
        self.crawl_page('First spider', Spider.base_url)


    # Creates directory and files for project on first run and starts the spider
    @staticmethod
    def initialize():

        Spider.queue.add(Spider.base_url)
        Spider.es = Elasticsearch(hosts=[{'host': Spider.host, 'port': 443}],
                                         http_auth=Spider.awsauth,
                                         use_ssl=True,
                                         verify_certs=True,
                                         connection_class=RequestsHttpConnection)

        Spider.es.indices.create(index='craigslist-index', ignore=400)

    # Updates user display, fills queue and updates files
    @staticmethod
    def crawl_page(thread_name, page_url):
        if page_url not in Spider.crawled:
            print(thread_name + ' now crawling ' + page_url)
            print('Queue ' + str(len(Spider.queue)) + ' | Crawled  ' + str(len(Spider.crawled)))
            Spider.add_links_to_queue(Spider.gather_links(page_url))
            Spider.pushToElasticsearch(page_url)
            Spider.queue.remove(page_url)
            Spider.crawled.add(page_url)


    @staticmethod
    def pushToElasticsearch(page_url):
        html_string = ''
        try:
            response = urlopen(page_url)
            if 'text/html' in response.getheader('Content-Type'):
                html_bytes = response.read()
                html_string = html_bytes.decode("utf-8")
            soup = BeautifulSoup(html_string, "html.parser")
            body_tag = soup.body['class']
            #Check body tag for class attr of posting
            if('posting' in body_tag):
                #Collect data from craigslist webpage
                title = soup.find("span", id="titletextonly").string
                postingBodyTag = soup.find("section", id="postingbody")
                postingBodyTag.div.extract() # Remove unnecessary tag inside section of postingbody
                description = postingBodyTag.get_text()
                result = Spider.es.index(index="craigslist-index", doc_type="test-type", body={"link": page_url, "description": description, "title": title, "timestamp": datetime.now()})
                #print(result)

        except Exception as e:
            print(str(e))
            return False
        return True


    # Converts raw response data into readable information and checks for proper html formatting
    @staticmethod
    def gather_links(page_url):
        html_string = ''
        try:
            response = urlopen(page_url)
            if 'text/html' in response.getheader('Content-Type'):
                html_bytes = response.read()
                html_string = html_bytes.decode("utf-8")
            finder = LinkFinder(Spider.base_url, page_url)
            finder.feed(html_string)

        except Exception as e:
            print(str(e))
            return set()
        return finder.page_links()


    @staticmethod
    def add_links_to_queue(links):
        for url in links:
            if (url in Spider.queue) or (url in Spider.crawled):
                continue
            if Spider.domain_name != get_domain_name(url):
                continue
            Spider.queue.add(url)
