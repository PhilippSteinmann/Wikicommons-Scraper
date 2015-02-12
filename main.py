# Adapted from http://www.ibm.com/developerworks/aix/library/au-threadingpython/
import Queue
import threading
import urllib2
import os
import urllib
import time
import string
import csv
import random
import re
import sys
from bs4 import BeautifulSoup

# A thread is a part of a program thar can run simultaneously with others
NUM_CATEGORY_THREADS = 40
NUM_PAINTING_THREADS = 40

# URL to start out with
initial_url = "http://commons.wikimedia.org/wiki/Category:1527_paintings"

download_images = False

# The order of fields that the CSV will be written in
csv_fields = ["artist", "title", "date", "medium", "dimensions", "file_name", "file_url", "description_url"]

# If the user specified URL, use that
if len(sys.argv) >= 2:
    initial_url = sys.argv[1]

if len(sys.argv) >= 3:
    if sys.argv[2] == "download":
        download_images = True
    elif sys.argv[2] == "nodownload":
        download_images = False
    else:
        print "USAGE: python main.py <URL> <download/nodownload>"
        sys.exit()

# This thread takes category_urls from category_url_queue and fetches & parses them
# Adds new categories and paintings that it finds
class FetchCategory(threading.Thread):
    def __init__(self, category_url_queue, painting_url_queue, category_urls_retrieved, category_urls_failed_to_retrieve):
        threading.Thread.__init__(self)
        self.category_url_queue = category_url_queue
        self.painting_url_queue = painting_url_queue
        self.category_urls_retrieved = category_urls_retrieved
        self.category_urls_failed_to_retrieve = category_urls_failed_to_retrieve
        self.base_url = "http://commons.wikimedia.org" 

    # Parse page for category links
    def findOtherCategories(self, soup):
        category_links = soup.select("a.CategoryTreeLabel.CategoryTreeLabelNs14.CategoryTreeLabelCategory")
        links = [self.base_url + link["href"] for link in category_links]
        return links

    # Parse page for painting links
    def findPaintings(self, soup):
        painting_links = soup.select(".gallery.mw-gallery-traditional a.image")
        links = [self.base_url + link["href"] for link in painting_links]
        return links

    def run(self):
        while True:
            # Pop from queue
            category_url = self.category_url_queue.get()
            print "Category #%d: Looking at %s" % (len(self.category_urls_retrieved) + 1, category_url)

            # Check if duplicate
            if category_url in self.category_urls_retrieved:
                print "Already fetched %s" % (category_url)
                self.category_url_queue.task_done()
                continue

            self.category_urls_retrieved.append(category_url)

            # Fetch HTML, feed to Beautiful Soup
            try:
                html = urllib2.urlopen(category_url).read()
            except:
                # If we've tried and failed to retrieve before, there's no point
                # in trying again. But if it's first failure, let's put it
                # back into queue and try again.
                if category_url not in self.category_urls_failed_to_retrieve:
                    self.category_url_queue.put(category_url)
                    self.category_urls_failed_to_retrieve.append(category_url)

                self.category_url_queue.task_done()
                continue

            soup = BeautifulSoup(html)

            # Get list of categories and paintings as list
            categories_in_page = self.findOtherCategories(soup)
            paintings_in_page = self.findPaintings(soup)

            # Put categories in page in categories queue
            for category in categories_in_page:
                self.category_url_queue.put(category)

            # Put paintings in page in paintings queue
            for painting in paintings_in_page:
                self.painting_url_queue.put(painting)

            self.category_url_queue.task_done()


# This thread takes painting_urls from painting_url_queue and fetches them,
# adds to metadata.csv, and downloads image file
class FetchPainting(threading.Thread):
    def __init__(self, painting_url_queue, file_obj, file_lock, painting_urls_retrieved, painting_urls_failed_to_retrieve, csv_writer):
        threading.Thread.__init__(self)
        self.painting_url_queue = painting_url_queue
        self.file_obj = file_obj
        self.file_lock = file_lock
        self.painting_urls_retrieved = painting_urls_retrieved
        self.painting_urls_failed_to_retrieve = painting_urls_failed_to_retrieve
        self.csv_writer = csv_writer

    def readMetaData(self, html):
        soup = BeautifulSoup(html)

        metadata_table = soup.select(".fileinfotpl-type-artwork")
        if len(metadata_table) == 0:
            return False

        # Get all <spans> with ID "creator"
        artist_list = soup.select("span#creator")
        if len(artist_list) == 0:
            artist = ""
        else:
            artist = artist_list[0].string
            if artist == None:
                artist = ""

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
        return { "artist": artist, "title": title, "date":date, "medium": medium, "dimensions": dimensions, "file_url":file_url}

    def readMetaDataField(self, sibling_field_id, soup):
        sibling_elem = soup.select(sibling_field_id)
        if len(sibling_elem) != 1 or sibling_elem[0].next_sibling == None:
            return ""
        field_elem = sibling_elem[0].next_sibling.next_sibling
        field_value = ''.join(field_elem.findAll(text=True))
        field_value = string.rstrip(field_value)
        return field_value


    def generateFileName(self, metadata):
        file_extension = metadata["file_url"][-4:]
        random_part = ''.join(random.choice(string.digits) for i in range(6))
        if metadata["artist"] == "" or metadata["artist"] == None or "artist" not in metadata:
            artist_name = "Unkown_Artist"
        else:
            artist_name = self.path_safe(metadata["artist"])

        return artist_name + "_" + random_part + file_extension

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
            print "Painting #%d: Looking at %s" % (len(self.painting_urls_retrieved) + 1, painting_url)

            # If we've retrieved image before, don't repeat
            if painting_url in self.painting_urls_retrieved:
                print "Already fetched %s" % (painting_url)
                self.painting_url_queue.task_done()
                continue


            try:
                html = urllib2.urlopen(painting_url).read()
            except:
                # If we've tried and failed to retrieve before, there's no point
                # in trying again. But if it's first failure, let's put it
                # back into queue and try again.
                if painting_url not in self.painting_urls_failed_to_retrieve:
                    self.painting_url_queue.put(painting_url)
                    self.painting_urls_failed_to_retrieve.append(painting_url)

                self.painting_url_queue.task_done()
                continue
    
            # Read metadata from HTML
            metadata = self.readMetaData(html)
            if not metadata:
                print "Exiting for lack of metadata at %s" % (painting_url)
                self.painting_url_queue.task_done()
                continue

            file_url = metadata["file_url"]
            
            self.painting_urls_retrieved.append(file_url)

            file_name = self.generateFileName(metadata)

            metadata["file_name"] = "images/ " + file_name
            metadata["description_url"] = painting_url
            #print metadata

            if download_images:
                # Write image file to images/ directory
                urllib.urlretrieve(file_url, "images/" + file_name)

            # Encode dictionary to UTF-8, because many works have special characters
            metadata = {k:v.encode('utf8') for k,v in metadata.items()}

            # Lock needed to prevent mess when multiple threads are writing
            # If lock is locked, will wait until released
            self.file_lock.acquire()
            self.csv_writer.writerow(metadata)
            self.file_lock.release()

            self.painting_url_queue.task_done()
            print "Successfully fetched %s" % (painting_url)


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

    category_urls_retrieved = []
    category_urls_failed_to_retrieve = []

    # Keep track to prevent duplication
    print "=" * 50
    print "Spawning %d threads to search all categories..." % (NUM_CATEGORY_THREADS)
    print "=" * 50
    print

    category_url_queue.put(initial_url)
    # Spawn a pool of threads, and pass them the queue instance
    for i in range(NUM_CATEGORY_THREADS):
        category_thread = FetchCategory(category_url_queue, painting_url_queue, category_urls_retrieved, category_urls_failed_to_retrieve)
        category_thread.setDaemon(True)
        category_thread.start()


    painting_urls_retrieved = []
    painting_urls_failed_to_retrieve = []

    # Create lock for file
    file_lock = threading.Lock()

    # Open CSV file for appending
    file_obj = open("metadata.csv", "a+")

    if not os.path.exists("images/"):
        os.makedirs("images/")

    # Needed to convert dictionary -> CSV
    csv_writer = csv.DictWriter(file_obj, csv_fields)
    csv_writer.writeheader()
    time.sleep(0.5)

    print
    print "=" * 50
    print "Spawning %d threads to download all paintings..." % (NUM_PAINTING_THREADS)
    print "=" * 50
    print

    for i in range(NUM_PAINTING_THREADS):
        painting_thread = FetchPainting(painting_url_queue, file_obj, file_lock, painting_urls_retrieved, painting_urls_failed_to_retrieve, csv_writer)
        painting_thread.setDaemon(True)
        painting_thread.start()

    painting_url_queue.join()
    category_url_queue.join()
    file_obj.close()

if __name__ == "__main__":
    main()
