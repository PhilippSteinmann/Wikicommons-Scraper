# -*- coding: utf-8 -*-

import Queue
import threading
import urllib2
import sys
import re
import csv
from bs4 import BeautifulSoup

import recursive

NUM_THREADS = 30
INPUT_FILE = "../get_search/search_results.csv"
OUTPUT_FILE = "metadata.csv"

INPUT_SEPARATOR = ","
SEPARATOR = "$&"

csv_fields_successful = ["problems", "artist", "artist_normalized", "title", "date", "medium", "dimensions", "categories", "file_name", "file_url", "description_url"]
csv_fields_rejected = ["problems", "artist", "artist_normalized", "title", "date", "medium", "dimensions", "categories", "file_name", "file_url", "description_url"]
problems_that_are_okay = ["taken with camera","missing medium", "missing dimensions"]

download_images = False
if len(sys.argv) >= 2:
    if sys.argv[1] == "download":
        download_images = True
    elif sys.argv[1] == "nodownload":
        download_images = False
    else:
        print "USAGE: python get_metadata.py <download/nodownload>"
        sys.exit()

def read_urls():
    urls_queue = Queue.Queue()

    file_obj = open(INPUT_FILE)

    urls = file_obj.readlines()
    urls = urls[1:]
    for url in urls:
        url = url.strip("\n")
        url_split = url.split(INPUT_SEPARATOR)
        if len(url_split) != 2:
            continue

        painting_url, cirrus_url = url_split
        if painting_url[0] == "\"":
            painting_url = painting_url[1:]
        if painting_url[-1] == "\"":
            painting_url = painting_url[:-1]

        if "wikimedia" in cirrus_url:
            painting_url = "http://commons.wikimedia.org" + painting_url
        elif "wikipedia" in cirrus_url:
            painting_url = "http://en.wikipedia.org" + painting_url
        else:
            continue

        urls_queue.put(painting_url)

    file_obj.close()

    return urls_queue

def fetch_metadata(urls_queue):
    # Used to prevent duplicates
    painting_urls_retrieved = []

    # Used to try again if HTTP request fails
    painting_urls_failed_to_retrieve = []
    painting_counters = [0, 0]

    file_successful = open("metadata.csv", "a+")
    file_rejected = open("failed.csv", "a+")

    #csv_writer_successful = csv.DictWriter(successful_file, csv_fields_successful)
    #csv_writer_successful.writeheader()

    #csv_writer_rejected = csv.DictWriter(rejected_file, csv_fields_rejected)
    #csv_writer_rejected.writeheader()

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

    for i in range(NUM_THREADS):
        painting_thread = recursive.FetchPainting(urls_queue, successful_lock, rejected_lock, painting_urls_retrieved, painting_urls_failed_to_retrieve, file_successful, file_rejected, painting_counters, painting_counter_lock, problems_that_are_okay)
        painting_thread.setDaemon(True)
        painting_thread.start()

        urls_queue.join()


def main():
    urls_queue = urls = read_urls()
    fetch_metadata(urls_queue)

if __name__ == "__main__":
    main()
