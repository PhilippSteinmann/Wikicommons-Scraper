# -*- coding: utf-8 -*-

import Queue
import threading
import urllib2 
import json
import sys
import httplib

NUM_THREADS = 30
INPUT_FILE = "../data/artist_list.csv"
OUTPUT_FILE = "../data/title_list.csv"
XAPP_TOKEN = "JvTPWe4WsQO-xqX6Bts49nTqNSFJt70Gq74yNrstrihK_lL53aaVs3fbWRlnTA9evHsoYynWPz5in2MY5IBhuoxaqXlxh_cDV8ThHwxDBILY9RjROQUQK0KoMksobvaJorW9WvBRcZb6L8Tal96Ue-WvbQJ367lnLA77uQ1f82xCp-g8XkZ2felvikCzcM4vCmmQtz713z56YRFNMTpr4F8ihrDR6eS-mpdeov53N9w="

# Separator to use when parsing and when printing CSVs
SEPARATOR = "%"

# How many works per page to request
WORKS_PER_PAGE = 60

# Which mediums to filter with
MEDIUMS = ["painting", "work-on-paper", "drawing", "prints", "photography"] #photography?

# Order of CSV writing
FIELDS = ["title", "id", "image_url", "year", "medium", "dimensions", "category", "artist", "artist_birthday", "artist_nationality", "artist_id"]

def read_artists():
    file_obj = open(INPUT_FILE)

    artist_list = file_obj.readlines()
    artist_list = artist_list[1:]
    artist_queue = Queue.Queue()

    for artist in artist_list:
        artist = artist.strip("\n")
        artist_queue.put(artist)

    file_obj.close()
    return artist_queue

class FetchArtist(threading.Thread):
    def __init__(self, artist_queue, file_obj, write_lock):
        threading.Thread.__init__(self)
        self.artist_queue = artist_queue
        self.file_obj = file_obj
        self.write_lock = write_lock

    # Given parameters, generate API request URL
    def generate_url(self, base_url, page_number, medium):
        return "https://api.artsy.net/api/v1/search/filtered" + base_url + "?medium=" + medium + "&size=" + str(WORKS_PER_PAGE) + "&page=" + str(page_number)

    # Fetch JSON from given URL.
    def fetch_json(self, url):
        headers = { "X-XAPP-TOKEN": XAPP_TOKEN }
        request = urllib2.Request(url, None, headers)
        try:
            response = urllib2.urlopen(request)
        except Exception as e:
            # Try once more
            try:
                print e
                response = urllib2.urlopen(request)
            except Exception as e:
                # Give up
                print e
                print "FAILED URL: " + url
                return None

        return response.read()

    # Turn dictionary into format we need
    def clean_works(self, works_raw):
        works = []
        for work in works_raw:
            new_work = {}
            new_work = self.add_field(work["title"], "title", new_work)
            new_work = self.add_field(work["id"], "id", new_work)

            new_work = self.add_image_url(work, new_work)

            new_work = self.add_field(work["date"], "year", new_work)
            new_work = self.add_field(work["medium"], "medium", new_work)
            new_work = self.add_field(work["dimensions"]["in"], "dimensions", new_work)
            new_work = self.add_field(work["category"], "category", new_work)
            new_work = self.add_field(work["artist"]["name"], "artist", new_work)
            new_work = self.add_field(work["artist"]["birthday"], "artist_birthday", new_work)
            new_work = self.add_field(work["artist"]["nationality"], "artist_nationality", new_work)
            new_work = self.add_field(work["artist"]["id"], "artist_id", new_work)

            works.append(new_work)
        
        return works

    # Look whether `field` exists. If yes, UTF-8-encode, if no, put placeholder string
    def add_field(self, field, name, work):
        if field == None:
            field = "none"
        else:
            field = field.encode("utf-8")
        work[name] = field
        return work
    
    # Extract the image URL from response.
    def add_image_url(self, work, new_work):
        if not ("images" in work and 
                len(work["images"]) > 0 and 
                "image_versions" in work["images"][0] and 
                work["images"][0]["image_versions"]):
            new_work = self.add_field(None, "image_url", new_work)
            return new_work

        if "larger" in work["images"][0]["image_versions"]:
            new_work = self.add_field(work["images"][0]["image_urls"]["larger"], "image_url", new_work)
        elif "large" in work["images"][0]["image_versions"]:
            new_work = self.add_field(work["images"][0]["image_urls"]["large"], "image_url", new_work)
        else:
            if len(work["images"][0]["image_urls"]) == 0:
                new_work = self.add_field(None, "image_url", new_work)
            else:
                image_version = work["images"][0]["image_versions"][-1]
                new_work = self.add_field(work["images"][0]["image_urls"][image_version], "image_url", new_work)

        return new_work

    # Turn collected metadata for all works from artist into one long string (with linebreaks)
    def generate_string(self, works):
        string = ""
        for work in works:
            for field in FIELDS:
                string += " ".join(work[field].replace("\n","").split()) + SEPARATOR

            string = string[:-len(SEPARATOR)]
            string += "\n"

        #string = string[:-1]
        return string.replace("\t","")

    def write_works_to_file(self, works):
        if len(works) == 0:
            return

        works_str = self.generate_string(works)
        self.write_lock.acquire()
        self.file_obj.write(works_str)
        self.write_lock.release()

    def run(self):
        while True:
            artist_raw = self.artist_queue.get()
            artist_split = artist_raw.split(SEPARATOR)

            if len(artist_split) != 2:
                print "FAULTY INPUT: " + artist_raw
                self.artist_queue.task_done()
                continue

            name, base_url = artist_split
            print name

            for medium in MEDIUMS:
                #print name + ": " + medium
                page_number = 1

                while True:
                    #print name + ": " + medium + ": " + "page %d" % (page_number)
                    url = self.generate_url(base_url, page_number, medium)
                    json_str = self.fetch_json(url)

                    try:
                        works_raw = json.loads(json_str)
                    except (TypeError, ValueError) as e:
                        print e
                        print "FAILED TO PARSE JSON: " + url
                        break

                    if not works_raw:
                        break

                    works = self.clean_works(works_raw)
                    self.write_works_to_file(works)

                    if len(works) > 0:
                        page_number += 1
                    else:
                        break

            self.artist_queue.task_done()


def write_header(file_obj):
    header = SEPARATOR.join(FIELDS)
    file_obj.write(header)

def set_xapp():
    if len(sys.argv) >= 2:
        XAPP_TOKEN = sys.argv[2]

def fetch_titles(artist_queue):
    set_xapp()
    file_obj = open(OUTPUT_FILE, "w+")
    write_lock = threading.Lock()

    write_header(file_obj)

    threads = []
    for i in range(NUM_THREADS):
        new_thread = FetchArtist(artist_queue, file_obj, write_lock)
        new_thread.setDaemon(True)
        new_thread.start()
        threads.append(new_thread)
    
    try:
        while sum([i.isAlive() for i in threads]):
            pass
    except KeyboardInterrupt:
        print "Terminating..."
        exit()

    file_obj.close()

def main():
    artist_queue = read_artists()
    fetch_titles(artist_queue)

if __name__ == "__main__":
    main()

