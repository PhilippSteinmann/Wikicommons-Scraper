# -*- coding: utf-8 -*-

import Queue
import threading
import urllib2
import urllib
import sys
import csv
import re
from bs4 import BeautifulSoup

NUM_THREADS = 30
FILE_NAME = "search_results.csv"
WORKS_PER_PAGE = 500

class FetchPage(threading.Thread):
    def __init__(self, base_url, template, search_parameters, write_lock):
        threading.Thread.__init__(self)
        self.base_url = base_url
        self.template = template
        self.search_parameters = search_parameters
        self.write_lock = write_lock

    def give_offset_queue(self, offset_queue):
        self.offset_queue = offset_queue

    def give_csv_writer(self, csv_writer):
        self.csv_writer = csv_writer

    def construct_url(self, offset):
        http_params = {}
        http_params["limit"] = WORKS_PER_PAGE
        http_params["offset"] = offset
        http_params["profile"] = "images"
        http_params["search"] = "hastemplate:\"" + self.template + "\" "
        http_params["search"] += self.search_parameters
        encoded_params = urllib.urlencode(http_params)
        
        full_url = self.base_url + "?" + encoded_params
        return full_url

    def fetch_html(self, url, offset):
        try:
            html = urllib2.urlopen(url).read()
            soup = BeautifulSoup(html)
            return soup

        except:
            print "Failed URL: %s" % (url)
            self.offset_queue.put(offset)
            self.offset_queue.task_done()
            return False

    def find_works(self, soup, find_page_length=False):
        urls = []

        work_tables = soup.select("table.searchResultImage")
        work_links = soup.select("table.searchResultImage a")
        for work_link in work_links:
            url = work_link["href"]
            urls.append(url)

        if not find_page_length:
            return urls
        
        page_length_container = soup.select("div.results-info")[0]
        all_children_strings = [tag.string for tag in page_length_container.contents]
        full_string = "".join(all_children_strings)

        page_length_index = full_string.index("of") + 3
        page_length_string = full_string[page_length_index:]
        page_length_string = page_length_string.replace(",", "")
        page_length = int(page_length_string)

        return (urls, page_length)


    def generate_other_offsets(self, page_length):
        current_offset = WORKS_PER_PAGE
        while current_offset <= page_length:
            self.offset_queue.put(current_offset)
            current_offset += WORKS_PER_PAGE

    def make_dictionary(self, works, search_url):
        works_old = works
        works = []
        for work in works_old:
            work_dict = {}
            work_dict["url"] = work
            work_dict["search_url"] = search_url
            works.append(work_dict)

        return works

    def run(self):
        while True:
            offset = self.offset_queue.get()

            url = self.construct_url(offset)
            soup = self.fetch_html(url, offset)
            if not soup:
                continue

            if offset == 0:
                print "Getting first page of set..."
                works, page_length = self.find_works(soup, find_page_length=True)
                self.generate_other_offsets(page_length)
            else:
                works = self.find_works(soup)

            works = self.make_dictionary(works, url)
            
            self.write_lock.acquire()
            for work in works:
                self.csv_writer.writerow(work)
            self.write_lock.release()

            self.offset_queue.task_done()


def generate_all_threads():
    thread_sets = []

    base_urls = ["http://commons.wikimedia.org/w/index.php", "http://en.wikipedia.org/w/index.php"]
    templates = ["artwork"]
    search_parameters = ["oil OR painting OR canvas OR watercolor OR watercolour OR woodblock OR etching OR tempera OR acrylic OR chalk OR charcoal OR pencil OR graphite OR conté OR crayon OR gouache OR pastel"] #ink? pen? paper? vellum? photography?
    
    for base_url in base_urls:
        for template in templates:
            for search_parameter in search_parameters:
                thread_set = generate_thread_set(base_url, template, search_parameter)
                thread_sets.append(thread_set)

    return thread_sets

def generate_thread_set(base_url, template, search_parameter):
    threads = []
    write_lock = threading.Lock()
    for i in range(NUM_THREADS):
        thread = FetchPage(base_url, template, search_parameter, write_lock)
        thread.setDaemon(True)
        threads.append(thread)

    return threads

def run_all_threads(thread_sets):
    file_obj = open(FILE_NAME, "a+")

    csv_fields = ["url", "search_url"]
    csv_writer = csv.DictWriter(file_obj, csv_fields)
    csv_writer.writeheader()

    for index, thread_set in enumerate(thread_sets):
        print "Running thread set #%d..." % (index + 1)
        run_thread_set(thread_set, csv_writer)

def run_thread_set(thread_set, csv_writer):
    offset_queue = Queue.Queue()
    offset_queue.put(0)

    for thread in thread_set:
        thread.give_csv_writer(csv_writer)
        thread.give_offset_queue(offset_queue)
        thread.start()

    offset_queue.join()

def main():
    thread_sets = generate_all_threads()
    run_all_threads(thread_sets)

if __name__ == "__main__":
    main()
