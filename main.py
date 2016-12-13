import threading
from queue import Queue
from spider import Spider
from domain import *

START = 'http://website.address.com/'
DOMAIN_NAME = get_domain_name(START)
NUMBER_OF_THREADS = 8
queue = Queue()
Spider(START, DOMAIN_NAME)


# Create worker threads (will die when main exits)
def create_workers():
    for _ in range(NUMBER_OF_THREADS):
        t = threading.Thread(target=work)
        t.daemon = True
        t.start()


# Do the next job in the queue
def work():
    while True:
        url = queue.get()
        Spider.crawl_page(threading.current_thread().name, url)
        queue.task_done()


# Each queued link is a new job
def create_jobs():
    #print("create_jobs()")
    for link in Spider.queue:
        queue.put(link)
    queue.join()
    crawl()


# Check if there are items in the queue, if so crawl them
def crawl():
    #print("crawl()")
    queued_links = Spider.queue
    if len(queued_links) > 0:
        print(str(len(queued_links)) + ' links in the queue')
        create_jobs()


create_workers()
crawl()
