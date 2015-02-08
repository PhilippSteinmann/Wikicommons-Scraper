# Adapted from http://www.ibm.com/developerworks/aix/library/au-threadingpython/
import Queue
import threading
import urllib2
import urllib
import time
import csv
from bs4 import BeautifulSoup

# URL to start out with
initial_url = "http://commons.wikimedia.org/wiki/Category:1367_paintings"

# A queue containing category pages to be scraped
category_url_queue = Queue.queue()

# A queue containing painting pages to be scraped
painting_url_queue = Queue.queue(i)

# The order of fields that the CSV will be written in
csv_fields = ["file_name", "file_url"]

# This thread takes category_urls from category_url_queue and fetches & parses them
# Adds new categories and paintings that it finds
class FetchCategory(threading.Thread):
    def __init__(self, category_url_queue, painting_url_queue):
        threading.Thread.__init__(self)
        self.category_url_queue = category_url_queue
        self.painting_url_queue = painting_url_queue

    # Parse page
    def findOtherCategories(self, html):
        return (,)

    # Parse page
    def findPaintings(self, html):
        return (,)

    def run(self):
        while True:
            # Pop from queue
            category_url = self.category_url_queue.get()
            html = urllib2.urlopen(category_url).read()

            categories_in_page = self.findOtherCategories(html)
            paintings_in_page = self.findPaintings(html)

            # Put categories in page in categories queue
            for category in categories_in_page:
                self.category_url_queue.put(category)

            # Put paintings in page in paintings queue
            for painting in paintings_in_page:
                self.painting_url_queue.put(painting)

            self.category_url_queue().task_done()

# This thread takes painting_urls from painting_url_queue and fetches them,
# Adds to metadata.csv, and downloads image file
class FetchPainting(threading.Thread):
    def __init__(self, painting_url_queue, file_obj, file_lock):
        threading.Thread.__init__(self)
        self.painting_url_queue = painting_url_queue
        self.file_obj = file_obj
        self.file_lock = file_lock

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
            file_name = self.generateFileName(file_url)

            metadata["file_name"] = "images/ " + file_name
            # Write image file to images/ directory
            urllib.urlretrieve(file_url, "images/" + file_name)

            # Lock needed to prevent mess when multiple threads are writing
            # If lock is locked, will wait until released
            self.lock.acquire()
            csv_writer.writerow(metadata)
            self.lock.release()

def main():
    # Create lock for file
    file_lock = threading.Lock()

    # Open CSV file for appending
    file_obj = open("metadata.csv", "a+")

    # Needed to convert dictionary -> CSV
    csv_writer = csv.DictWriter(file_obj, csv_fields)
    csv_writer.writeheader()

    # Spawn a pool of threads, and pass them queue instance
    for i in range(5):
        category_thread = ThreadUrl(category_url_queue, painting_url_queue)
        category_thread.setDaemon(True)
        category_thread.start()

    category_queue.put(initial_url)

    for i in range(5):
        painting_thread = DatamineThread(painting_url_queue, file_obj, file_lock)
        painting_thread.setDaemon(True)
        painting_thread.start()

    category_url_queue.join()
    painting_url_queue.join()
    file_obj.close()

if __name__ == "__main__":
    main()

main()
