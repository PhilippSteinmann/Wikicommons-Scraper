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
XAPP_TOKEN = "JvTPWe4WsQO-xqX6Bts49qjIwrgaUqjiFZJCEm7fMvVUEorceDZTN2s0lz_XhIw6oOQhs0l2XQWey0f0w1Mg6DodbYChO0uV3NgmyNuF2pglVlFnhaa5Wol48sWtLswT2uAWdfQpxTa4oN_MBJsI5bu1oyHRDdvzXJw1UUZPqn1-rquuP_iJPSBrCmbVhkh4GIjG0p8CFVnGwyx1tR56K966y7JIQ4HULxihqbD4AF8="
SEPARATOR = "%"
WORKS_PER_PAGE = 60
MEDIUMS = ["work-on-paper", "drawing", "prints", "photography"]
FIELDS = ["title", "id", "image_url", "year", "medium", "dimensions", "artist", "artist_birthday", "artist_nationality", "artist_id"]

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

    def generate_url(self, base_url, page_number, medium):
        return "https://api.artsy.net/api/v1/search/filtered" + base_url + "?medium=" + medium + "&size=" + str(WORKS_PER_PAGE) + "&page=" + str(page_number)

    def fetch_json(self, url):
        headers = { "X-XAPP-TOKEN": XAPP_TOKEN }
        request = urllib2.Request(url, None, headers)
        try:
            response = urllib2.urlopen(request)
        except (urllib2.URLError, httplib.BadStatusLine) as e:
            try:
                print e
                response = urllib2.urlopen(request)
            except (urllib2.URLError, httplib.BadStatusLine) as e:
                print e
                print "FAILED URL: " + url
                return None

        return response.read()

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
            new_work = self.add_field(work["artist"]["name"], "artist", new_work)
            new_work = self.add_field(work["artist"]["birthday"], "artist_birthday", new_work)
            new_work = self.add_field(work["artist"]["nationality"], "artist_nationality", new_work)
            new_work = self.add_field(work["artist"]["id"], "artist_id", new_work)

            works.append(new_work)
        
        return works

    def add_field(self, field, name, work):
        if field == None:
            field = "none"
        else:
            field = field.encode("utf-8")
        work[name] = field
        return work
        
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

    def generate_string(self, works):
        string = ""
        for work in works:
            for field in FIELDS:
                string += work[field] + SEPARATOR

            string = string[:-2]
            string += "\n"

        string = string[:-2]
        return string

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
            name, base_url = artist_raw.split(SEPARATOR)
            print name

            for medium in MEDIUMS:
                page_number = 1

                while True:
                    url = self.generate_url(base_url, page_number, medium)
                    json_str = self.fetch_json(url)

                    try:
                        works_raw = json.loads(json_str)
                    except ValueError as e:
                        print e
                        continue

                    if not works_raw:
                        continue

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

    for i in range(NUM_THREADS):
        new_thread = FetchArtist(artist_queue, file_obj, write_lock)
        new_thread.setDaemon(True)
        new_thread.start()
    
    artist_queue.join()
    file_obj.close()

def main():
    artist_queue = read_artists()
    fetch_titles(artist_queue)

if __name__ == "__main__":
    main()

