# -*- coding: utf-8 -*-

# Thanks to http://www.ibm.com/developerworks/aix/library/au-threadingpython/
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
from urlparse import urlparse
from bs4 import BeautifulSoup

# A thread is a part of a program thar can run simultaneously with others
NUM_CATEGORY_THREADS = 60
NUM_PAINTING_THREADS = 60

# It's useful to keep track of which paintings we reject because of bad metadata
LOG_REJECTED_PAINTINGS = True

# Anything later is rejected
MAXIMUM_PAINTING_DATE = 1980

# If file is greater, won't be downloaded. 20 MB
MAX_FILE_SIZE = 20 * 1000 * 1000

# If file size is less, won't be downloaded. 40 KB
MIN_FILE_SIZE = 40 * 1000

# Should we care about file size? Slows down program
LIMIT_FILE_SIZE = True

# URL to start from
initial_url = "http://commons.wikimedia.org/wiki/Category:Paintings"

# Used for testing, and to name files
# First is successful, second is rejected
# Using array because passing reference to threads is easy
painting_counters = [0, 0]

# If user specified own URL, use that
if len(sys.argv) >= 2:
    initial_url = sys.argv[1]

# If user specified image downloading, do so
download_images = False
if len(sys.argv) >= 3:
    if sys.argv[2] == "download":
        download_images = True
    elif sys.argv[2] == "nodownload":
        download_images = False
    else:
        print "USAGE: python main.py <URL> <download/nodownload> <restrictive/permissive>"
        sys.exit()

strictness = "permissive"
if len(sys.argv) >= 4:
    if sys.argv[3] == "strict" or sys.argv[3] == "permissive":
        strictness = sys.argv[3]
    else:
        print "USAGE: python main.py <URL> <download/nodownload> <strict/permissive>"
        sys.exit()

if strictness == "strict":
    problems_that_are_okay = ["taken with camera"]
else:
    problems_that_are_okay = ["taken with camera", "missing artwork table", "empty artist", "missing artist wikipedia link", "missing date", "missing title", "missing medium", "missing dimensions", "too recent", "detail of painting", "missing file URL"]

# The order of fields that the CSV will be written in
csv_fields_successful = ["problems", "artist", "artist_normalized", "title", "date", "medium", "dimensions", "categories", "file_name", "file_url", "description_url"]
csv_fields_rejected = ["problems", "artist", "artist_normalized", "title", "date", "medium", "dimensions", "categories", "file_name", "file_url", "description_url"]

# This is the first of two types of threads found in this program
# It takes a category_url from category_url_queue, fetches the HTML,
# looks for (1) more category links and (2) painting links
# Adds new categories to category_url_queue and paintings to painting_url_queue
class FetchCategory(threading.Thread):
    def __init__(self, category_url_queue, painting_url_queue, category_urls_retrieved, category_urls_failed_to_retrieve):
        threading.Thread.__init__(self)
        self.category_url_queue = category_url_queue
        self.painting_url_queue = painting_url_queue
        self.category_urls_retrieved = category_urls_retrieved
        self.category_urls_failed_to_retrieve = category_urls_failed_to_retrieve

    # http://stackoverflow.com/a/9626596/805556
    def get_domain_name(self, url):
        parsed_uri = urlparse(url)
        domain = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
        return domain

    # Parse page for category links
    def findOtherCategories(self, soup, category_url):
        # Very easy
        category_links = soup.select("a.CategoryTreeLabel.CategoryTreeLabelNs14.CategoryTreeLabelCategory")
        category_links += soup.select(".mw-category-group a")

        # Looking for a 'next 200' link is not so easy
        # Look for element that contains string "next 200"
        next_page_regex = re.compile(r'next 200')
        next_page_navigable_string = soup.find(text=next_page_regex)
        if next_page_navigable_string != None:
            next_page = next_page_navigable_string.parent
            if next_page.name == "a":
                category_links.append(next_page)

        # Links in HTML are often relative, add the base_url to make them absolute
        base_url = self.get_domain_name(category_url)
        links = [base_url + link["href"] for link in category_links]
        return links

    # Parse page for painting links
    def findPaintings(self, soup, category_url):
        painting_links = soup.select(".gallery.mw-gallery-traditional a.image")
        painting_links += soup.select("table.infobox.vevent a.image")

        # Links in HTML are often relative, add the base_url to make them absolute
        base_url = self.get_domain_name(category_url)
        links = [base_url + link["href"] for link in painting_links]
        return links

    # Main method of thread
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

            # Add to array of categories already checked
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

            # Use Beautiful Soup 4 to analyze HTML
            soup = BeautifulSoup(html)

            # Get list of categories and paintings as array
            categories_in_page = self.findOtherCategories(soup, category_url)
            paintings_in_page = self.findPaintings(soup, category_url)

            # Put new categories in categories queue
            for category in categories_in_page:
                self.category_url_queue.put(category)

            # Put new paintings in paintings queue
            for painting in paintings_in_page:
                self.painting_url_queue.put(painting)

            # Very important statement. At bottom of program we do 
            # `category_url_queue.join()`, which waits until all items in queue
            # Have been task_done'd.
            self.category_url_queue.task_done()


# This is the second of two types of threads found in this program
# Takes painting URLs from painting_url_queue and fetches HTML, looks at metadata,
# adds to metadata.csv, and downloads image file (if user wants to)
class FetchPainting(threading.Thread):
    def __init__(self, painting_url_queue, successful_file, successful_lock, rejected_file, rejected_lock, painting_urls_retrieved, painting_urls_failed_to_retrieve, csv_writer_successful, csv_writer_rejected, painting_counters, painting_counter_lock, problems_that_are_okay):
        threading.Thread.__init__(self)
        self.painting_url_queue = painting_url_queue
        self.successful_file = successful_file
        self.successful_lock = successful_lock
        self.rejected_file = rejected_file
        self.rejected_lock = rejected_lock
        self.painting_urls_retrieved = painting_urls_retrieved
        self.painting_urls_failed_to_retrieve = painting_urls_failed_to_retrieve
        self.csv_writer_successful = csv_writer_successful
        self.csv_writer_rejected = csv_writer_rejected
        self.painting_counters = painting_counters
        self.painting_counter_lock = painting_counter_lock
        self.problems_that_are_okay = problems_that_are_okay

    def normalized(self, artist):
        artist = artist.lower()
        artist = artist.replace("-", "")

        artist_names = artist.split(" ")
        if len(artist_names) >= 3:
            artist = artist_names[0] + artist_names[-1]

        artist = artist.replace(" ", "")
        new_artist_str = ""

        replacement_table = { ord(u"ä"): "a", ord(u"ü"): "u", ord(u"ö"): "o", ord(u"ß"): "ss", ord(u"é"): "e", ord(u"è"): "e", ord(u"à"): "a", ord(u"á"): "a",  ord(u"û"): "u", ord(u"ô"): "o"}
        for letter in artist:
            if ord(letter) in replacement_table:
                new_artist_str += replacement_table[ord(letter)]
            else:
                new_artist_str += letter

        artist = new_artist_str

        return artist

    # Important method, takes html and picks out metadata
    # Looks for problems with metadata
    def readMetaData(self, html):
        soup = BeautifulSoup(html)

        metadata = { "artist": "", "title": "", "date": "", "medium": "", "dimensions": "", "file_url":""}
        problems = []

        # The main metadata table.
        metadata_table = soup.select(".fileinfotpl-type-artwork")
        if len(metadata_table) == 0:
            problems.append("missing artwork table")

        # Find artist: get all <spans> with ID "creator"
        artist_list = soup.select("span#creator")
        artist = ""

        if len(artist_list) == 0:
            problems.append("missing artist")

        else:
            artist = artist_list[0].string
            if artist == None:
                artist = ""
                problems.append("empty artist")

            metadata["artist"] = artist

        metadata["artist_normalized"] = self.normalized(metadata["artist"])

        # Not only do we require name of artist, we require link back to artist's
        # Wikipedia page
        artist_wikipedia_link = soup.select("span#creator a")
        if artist != "Unkown" and len(artist_wikipedia_link) == 0:
            problems.append("missing artist wikipedia link")


        # These four fields are situated similarly in the HTML, so I made a single
        # easy function to fetch them.
        metadata["date"] = self.readMetaDataField("#fileinfotpl_date", soup)
        metadata["title"] = self.readMetaDataField("#fileinfotpl_art_title", soup)
        metadata["medium"] = self.readMetaDataField("#fileinfotpl_art_medium", soup)
        metadata["dimensions"] = self.readMetaDataField("#fileinfotpl_art_dimensions", soup)

        # If any field is missing, that's a problem
        if not metadata["date"]:
            problems.append("missing date")
        else:
            metadata["date"] = metadata["date"].replace("\n", " ")

        if not metadata["title"]:
            problems.append("missing title")
        else:
            metadata["title"] = metadata["title"].replace("\n", " ")

        if not metadata["medium"]:
            problems.append("missing medium")
        else:
            metadata["medium"] = metadata["medium"].replace("\n", " ")

        if not metadata["dimensions"]:
            problems.append("missing dimensions")
        else:
            metadata["dimensions"] = metadata["dimensions"].replace("\n", " ")

        # Check if painting is too new
        if metadata["date"] and metadata["date"].isdigit() and int(metadata["date"]) > MAXIMUM_PAINTING_DATE:
            problems.append("too recent")

        # Check if image is detail of painting
        if metadata["title"] and "detail" in metadata["title"]:
            problems.append("detail of painting")


        # Look for link to the full image file
        file_url_regex = re.compile(r'Original file')
        file_url_navigable_string = soup.find(text= file_url_regex)
        if file_url_navigable_string != None:
            file_url = file_url_navigable_string.parent["href"]
        else:
            file_url_elem = soup.select(".fullMedia a")
            if len(file_url_elem) == 0:
                problems.append("missing file URL")
            else:
                file_url = file_url_elem[0]["href"]

        file_url = self.fix_file_url(file_url)
        metadata["file_url"] = file_url

        # If two elements that are typical of photos are found, add that to problems
        # Example: http://commons.wikimedia.org/wiki/File:Louvre-Lens_-_Galerie_du_Temps_(2013)_-_203_-_RF_129_(E)_(Freddy_Driel).JPG
        if soup.select("#mw_metadata .exif-make") and soup.select("#mw_metadata .exif-model"):
            problems.append("taken with camera")

        # Look for list of categories that this painting belongs to
        category_links_list = soup.select("#catlinks #mw-normal-catlinks ul a")
        if len(category_links_list) > 0:
            category_names = [category.string for category in category_links_list]
            categories_as_string = "~".join(category_names)
            metadata["categories"] = categories_as_string

        return (metadata, problems)

    # Used to fetch date, title, dimensions, medium
    def readMetaDataField(self, sibling_field_id, soup):
        sibling_elem = soup.select(sibling_field_id)
        if len(sibling_elem) != 1 or sibling_elem[0].next_sibling == None:
            return False
        field_elem = sibling_elem[0].next_sibling.next_sibling
        field_value = ''.join(field_elem.findAll(text=True))
        field_value = string.rstrip(field_value)
        return field_value

    # Given metadata, create file name on disk
    def generateFileName(self, metadata, image_number):
        artist_name = metadata["artist_normalized"]
        if not artist_name or len(artist_name) == 0:
            artist_name = "unknown"

        file_extension = os.path.splitext(metadata["file_url"])[1]
        number_part = "{0:08d}".format(image_number)
        return artist_name + "_" + number_part + file_extension

    # Change spaces to underscores
    def path_safe(self, string):
        return string.replace(" ", "_")

    # Remove protocol ambiguity
    def fix_file_url(self, url):
        if url[0:2] == "//":
            url = "http:" + url
        return url

    # See how big image is
    def file_size_okay(self, file_url):
        # If user doesn't care, don't check
        if not LIMIT_FILE_SIZE:
            return True

        try:
            file_site = urllib2.urlopen(file_url)
        except:
            return True
        file_size_headers = file_site.info().getheaders("Content-Length")
        file_size_header = file_size_headers[0] if len(file_size_headers) > 0 else "0"
        file_size = int(file_size_header) if file_size_header.isdigit() else 0
        return (file_size <= MAX_FILE_SIZE and file_size >= MIN_FILE_SIZE)

    # Main method of thread
    def run(self): 
        while True:
            # Pop from queue
            painting_url = self.painting_url_queue.get()
            print "Painting #%d: Looking at %s" % (self.painting_counters[0] + self.painting_counters[1], painting_url)

            # If we've retrieved image before, don't repeat
            if painting_url in self.painting_urls_retrieved:
                print "Already fetched %s" % (painting_url)
                self.painting_url_queue.task_done()
                continue

            # Fetch HTML
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
            metadata, problems = self.readMetaData(html)
            metadata["description_url"] = painting_url

            # Encode dictionary to UTF-8, because many works have special characters
            metadata = { k:(v.encode('utf8') if v else "") for k,v in metadata.items() }

            file_url = metadata["file_url"]
            self.painting_urls_retrieved.append(file_url)

            # Certain problems with the metadata are tolerable

            # Check if problems with metadata are tolerable
            if len(problems) > 0:
                metadata["problems"] = "~".join(problems)
                problems_are_okay = True
                for problem in problems:
                    if not problem in self.problems_that_are_okay:
                        problems_are_okay = False
                        break

                if not problems_are_okay:
                    self.painting_counter_lock.acquire()
                    self.painting_counters[1] += 1
                    self.painting_counter_lock.release()

                    this_number = self.painting_counters[1]
                    file_name = self.generateFileName(metadata, this_number)
                    metadata["file_name"] = "failed_images/ " + file_name

                    print "Exiting for lack of metadata at %s" % (painting_url)
                    if LOG_REJECTED_PAINTINGS:
                        self.rejected_lock.acquire()
                        self.csv_writer_rejected.writerow(metadata)
                        self.rejected_lock.release()

                        if download_images and file_url:
                            if self.file_size_okay(file_url):
                                try:
                                    urllib.urlretrieve(file_url, "failed_images/" + file_name)
                                except:
                                    print "Unable to download %s" % (file_url)
                            else:
                                print "Exiting because file too large or small at %s" % (file_url)


                    self.painting_url_queue.task_done()
                    continue

            self.painting_counter_lock.acquire()
            self.painting_counters[0] += 1
            self.painting_counter_lock.release()

            this_number = self.painting_counters[0]
            file_name = self.generateFileName(metadata, this_number)
            metadata["file_name"] = "images/ " + file_name

            # If user wants to download images, do so
            if download_images:
                if self.file_size_okay(file_url):
                    try:
                        # Write image file to images/ directory
                        urllib.urlretrieve(file_url, "images/" + file_name)
                    except:
                        print "Unable to download %s" % (file_url)
                else:
                    print "Exiting because file too large or small at %s" % (file_url)

            # Lock needed to prevent mess when multiple threads are writing
            # If lock is locked, will wait until released
            self.successful_lock.acquire()
            self.csv_writer_successful.writerow(metadata)
            self.successful_lock.release()

            self.painting_url_queue.task_done()
            print "Successfully fetched %s" % (painting_url)

# No longer needed
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

    # Used to prevent duplicates
    category_urls_retrieved = []

    # Used to try again if HTTP request fails
    category_urls_failed_to_retrieve = []

    successful_file = open("metadata.csv", "a+")
    rejected_file = open("failed.csv", "a+")

    # Write BOM so that Excel can open
    #successful_file.write(u'\ufeff'.encode('utf8'))
    #rejected_file.write(u'\ufeff'.encode('utf8'))

    # Needed to convert dictionary -> CSV
    csv_writer_successful = csv.DictWriter(successful_file, csv_fields_successful)
    csv_writer_successful.writeheader()

    # Open CSV file for appending
    # Needed to convert dictionary -> CSV
    csv_writer_rejected = csv.DictWriter(rejected_file, csv_fields_rejected)
    csv_writer_rejected.writeheader()
    print "=" * 50
    print "Spawning %d threads to search all categories..." % (NUM_CATEGORY_THREADS)
    print "=" * 50
    print

    # Put initial URL to start things off
    category_url_queue.put(initial_url)

    all_threads = []
    # Spawn a pool of threads
    for i in range(NUM_CATEGORY_THREADS):
        category_thread = FetchCategory(category_url_queue, painting_url_queue, category_urls_retrieved, category_urls_failed_to_retrieve)
        category_thread.setDaemon(True)
        all_threads.append(category_thread)
        category_thread.start()


    # Used to prevent duplicates
    painting_urls_retrieved = []

    # Used to try again if HTTP request fails
    painting_urls_failed_to_retrieve = []

    # Create locks
    successful_lock = threading.Lock()
    rejected_lock = threading.Lock()
    painting_counter_lock = threading.Lock()


    # If user wants to download images, make sure directory exists
    if download_images and not os.path.exists("images/"):
        os.makedirs("images/")

    # If user wants to download rejected images, make sure directory exists
    if download_images and LOG_REJECTED_PAINTINGS and not os.path.exists("failed_images/"):
        os.makedirs("failed_images/")


    # Fixes weird issues
    time.sleep(0.5)

    print
    print "=" * 50
    print "Spawning %d threads to download all paintings..." % (NUM_PAINTING_THREADS)
    print "=" * 50
    print

    # Spawn a pool of threads
    for i in range(NUM_PAINTING_THREADS):
        painting_thread = FetchPainting(painting_url_queue, successful_file, successful_lock, rejected_file, rejected_lock, painting_urls_retrieved, painting_urls_failed_to_retrieve, csv_writer_successful, csv_writer_rejected, painting_counters, painting_counter_lock, problems_that_are_okay)
        painting_thread.setDaemon(True)
        all_threads.append(painting_thread)
        painting_thread.start()

    # The statements below are important. Program will not progress beyond these
    # two lines if both internal tickers don't reach 0. Tickers are incremented
    # when you put something queue, decremented when you do task_done()
    category_url_queue.join()
    painting_url_queue.join()

    successful_file.close()
    rejected_file.close() 

if __name__ == "__main__":
    main()
