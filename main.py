# Adapted from http://www.ibm.com/developerworks/aix/library/au-threadingpython/
import Queue
import threading
import urllib2
import urllib
import time
import csv
import re
import sys
from bs4 import BeautifulSoup

NUM_CATEGORY_THREADS = 1
NUM_PAINTING_THREADS = 1

# The order of fields that the CSV will be written in
csv_fields = ["title", "creator", "date", "file_name", "file_url", "description_url"]

# URL to start out with
initial_url = "http://commons.wikimedia.org/wiki/Category:1527_paintings"
if len(sys.argv) > 1:
    initial_url = sys.argv[1]

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
        painting_links = soup.select(".gallery.mw-gallery-traditional a.image")
        links = [self.base_url + link["href"] for link in painting_links]
        return links

    def run(self):
        while True:
            # Pop from queue
            category_url = self.category_url_queue.get()
            #print category_url + "\n"
            html = urllib2.urlopen(category_url).read()
            soup = BeautifulSoup(html)
            categories_in_page = self.findOtherCategories(soup)
            paintings_in_page = self.findPaintings(soup)
            # Put categories in page in categories queue
            for category in categories_in_page:
                self.category_url_queue.put(category)

            # Put paintings in page in paintings queue
            for painting in paintings_in_page:
                self.painting_url_queue.put(painting)
            #time.sleep(0.2)
            self.category_url_queue.task_done()


# This thread takes painting_urls from painting_url_queue and fetches them,
# adds to metadata.csv, and downloads image file
class FetchPainting(threading.Thread):
    def __init__(self, painting_url_queue, file_obj, file_lock, file_urls_retrieved, csv_writer):
        threading.Thread.__init__(self)
        self.painting_url_queue = painting_url_queue
        self.file_obj = file_obj
        self.file_lock = file_lock
        self.file_urls_retrieved = file_urls_retrieved
        self.csv_writer = csv_writer

    def readMetaData(self, html):
        soup = BeautifulSoup(html)

        metadata_table = soup.select(".fileinfotpl-type-artwork")
        if metadata_table == None:
            print "missing metadata table"
            return False

        creator_list = soup.select("span#creator")
        if len(creator_list) != 1:
            print "missing creator"
        creator = creator_list[0].string

        date = self.readMetaDataField("#fileinfotpl_date", soup)
        title = self.readMetaDataField("#fileinfotpl_art_title", soup)
        medium = self.readMetaDataField("#fileinfotpl_art_medium", soup)
        dimensions = self.readMetaDataField("#fileinfotpl_art_dimensions", soup)


        file_url_regex = re.compile(r'Original file')
        file_url_navigable_string = soup.find(text= file_url_regex)
        if file_url_navigable_string != None:
            file_url = file_url_navigable_string.parent["href"]
        else:
            file_url_elem = soup.select(".fullMedia a")
            if len(file_url_elem) == 0:
                print "missing file URL"
                return false
            file_url = file_url_elem[0]["href"]
        file_url = self.fix_file_url(file_url)
        return { "creator": creator, "title": title, "date":date, "file_url":file_url}

    def readMetaDataField(self, sibling_field_id, soup):
        sibling_elem = soup.select(sibling_field_id)
        if len(sibling_elem) != 1 or sibling_elem[0].next_sibling == None:
            return ""
        field_elem = sibling_elem[0].next_sibling.next_sibling
        field_value = ''.join(field_elem.findAll(text=True))
        return field_value


    def generateFileName(self, metadata):
        file_extension = metadata["file_url"][-4:]
        return self.path_safe(metadata["creator"]) + "_" + self.path_safe(metadata["title"]) + "_" + self.path_safe(metadata["date"]) + file_extension

    def path_safe(self, string):
        return string.replace(" ", "_")

    def fix_file_url(self, url):
        if url[0:2] == "//":
            url = "http:" + url
        return url

    def run(self): 
        while True:
            # Pop from queue
            painting_url = self.painting_url_queue.get()
            print painting_url
            html = urllib2.urlopen(painting_url).read()

            # Read metadata from HTML
            metadata = self.readMetaData(html)
            if not metadata:
                print "exiting for lack of metadata"
                self.painting_url_queue.task_done()
                continue

            file_url = metadata["file_url"]
            
            # If we've retrieved image before, don't repeat
            if file_url in self.file_urls_retrieved:
                print "already fetched"
                self.painting_url_queue.task_done()
                continue

            self.file_urls_retrieved.append(file_url)

            file_name = self.generateFileName(metadata)

            metadata["file_name"] = "images/ " + file_name
            metadata["description_url"] = painting_url
            print metadata
            # Write image file to images/ directory
            urllib.urlretrieve(file_url, "images/" + file_name)

            metadata = {k:v.encode('utf8') for k,v in metadata.items()}

            # Lock needed to prevent mess when multiple threads are writing
            # If lock is locked, will wait until released
            self.file_lock.acquire()
            self.csv_writer.writerow(metadata)
            self.file_lock.release()
            self.painting_url_queue.task_done()
            print
            print
            print


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
    # A queue containing category pages to be scraped
    category_url_queue = Queue.Queue()

    # A queue containing painting pages to be scraped
    painting_url_queue = Queue.Queue()

    # Keep track to prevent duplication
    file_urls_retrieved = []

    category_url_queue.put(initial_url)
    # Spawn a pool of threads, and pass them the queue instance
    for i in range(NUM_CATEGORY_THREADS):
        category_thread = FetchCategory(category_url_queue, painting_url_queue)
        category_thread.setDaemon(True)
        category_thread.start()

    category_url_queue.join()

    #painting_url_queue = removeDuplicates(painting_url_queue)
    #painting_url_queue.join()

    # Create lock for file
    file_lock = threading.Lock()

    # Open CSV file for appending
    file_obj = open("metadata.csv", "a+")

    # Needed to convert dictionary -> CSV
    csv_writer = csv.DictWriter(file_obj, csv_fields)
    csv_writer.writeheader()

    for i in range(NUM_PAINTING_THREADS):
        painting_thread = FetchPainting(painting_url_queue, file_obj, file_lock, file_urls_retrieved, csv_writer)
        painting_thread.setDaemon(True)
        painting_thread.start()

    painting_url_queue.join()
    file_obj.close()

if __name__ == "__main__":
    main()
