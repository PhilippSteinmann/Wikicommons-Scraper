# Adapted from http://www.ibm.com/developerworks/aix/library/au-threadingpython/
import Queue
import threading
import urllib2
import urllib
import time
import csv
from bs4 import BeautifulSoup

NUM_CATEGORY_THREADS = 15


# This thread takes category_urls from category_url_queue and fetches & parses them
# Adds new categories and paintings that it finds
class FetchCategory(threading.Thread):
    def __init__(self, category_url_queue, painting_url_queue):
        threading.Thread.__init__(self)
        self.category_url_queue = category_url_queue
        self.painting_url_queue = painting_url_queue
        self.base_url = "http://commons.wikimedia.org"

    # Parse page
    def findOtherCategories(self, soup):
        category_links = soup.select("a.CategoryTreeLabel.CategoryTreeLabelNs14.CategoryTreeLabelCategory")
        links = [self.base_url + link["href"] for link in category_links]
        return links

    # Parse page
    def findPaintings(self, soup):
        return ()

    def run(self):
        while True:
            # Pop from queue
            category_url = self.category_url_queue.get()
            #print category_url + "\n"
            html = urllib2.urlopen(category_url).read()
            self.category_url_queue.task_done()

            soup = BeautifulSoup(html)
            categories_in_page = self.findOtherCategories(soup)
            paintings_in_page = self.findPaintings(soup)

            # Put categories in page in categories queue
            for category in categories_in_page:
                self.category_url_queue.put(category)
                self.painting_url_queue.put(category)

            # Put paintings in page in paintings queue
            for painting in paintings_in_page:
                self.painting_url_queue.put(painting)
            #time.sleep(0.2)


# This thread takes painting_urls from painting_url_queue and fetches them,
# adds to metadata.csv, and downloads image file
class FetchPainting(threading.Thread):
    def __init__(self, painting_url_queue, file_obj, file_lock, file_urls_retrieved):
        threading.Thread.__init__(self)
        self.painting_url_queue = painting_url_queue
        self.file_obj = file_obj
        self.file_lock = file_lock
        self.file_urls_retrieved = file_urls_retrieved

    def readMetaData(self, html):
        return false

    def generateFileName(self, file_url):
        pass

    def run(self): 
        while True:
            # Pop from queue
            painting_url = self.painting_url_queue.get()
            html = urllib2.urlopen(painting_url).read()

            # Read metadata from HTML
            metadata = self.readMetaData(html)
            if not metadata:
                return

            file_url = metadata["file_url"]
            
            # If we've retrieved image before, don't repeat
            if file_url in file_urls_retrieved:
                return
            file_urls_retrieved.append(file_url)

            file_name = self.generateFileName(file_url)

            metadata["file_name"] = "images/ " + file_name
            # Write image file to images/ directory
            urllib.urlretrieve(file_url, "images/" + file_name)

            # Lock needed to prevent mess when multiple threads are writing
            # If lock is locked, will wait until released
            self.lock.acquire()
            csv_writer.writerow(metadata)
            self.lock.release()


def removeDuplicates(queue):
    newQueue = Queue.Queue()
    items_in_queue = []
    while not queue.empty():
        item = queue.get()
        items_in_queue.append(item)

    # Remove duplicates from list
    items_in_queue = list(set(items_in_queue))
    for item in items_in_queue:
        newQueue.put(item)

    return newQueue

def main():
    # URL to start out with
    initial_url = "http://commons.wikimedia.org/wiki/Category:1573_paintings"

    # A queue containing category pages to be scraped
    category_url_queue = Queue.Queue()

    # A queue containing painting pages to be scraped
    painting_url_queue = Queue.Queue()

    # The order of fields that the CSV will be written in
    csv_fields = ["file_name", "file_url"]

    # Keep track to prevent duplication
    file_urls_retrieved = []

    # Spawn a pool of threads, and pass them queue instance
    for i in range(NUM_CATEGORY_THREADS):
        category_thread = FetchCategory(category_url_queue, painting_url_queue)
        category_thread.setDaemon(True)
        category_thread.start()

    category_url_queue.put(initial_url)
    category_url_queue.join()
    painting_url_queue.join()

    while True:
        print painting_url_queue.get()

    painting_url_queue = removeDuplicates(painting_url_queue)

    # Create lock for file
    file_lock = threading.Lock()

    # Open CSV file for appending
    file_obj = open("metadata.csv", "a+")

    # Needed to convert dictionary -> CSV
    csv_writer = csv.DictWriter(file_obj, csv_fields)
    csv_writer.writeheader()

    for i in range(5):
        painting_thread = FetchPainting(painting_url_queue, file_obj, file_lock, file_urls_retrieved)
        painting_thread.setDaemon(True)
        #painting_thread.start()

    painting_url_queue.join()
    file_obj.close()

if __name__ == "__main__":
    main()

main()
