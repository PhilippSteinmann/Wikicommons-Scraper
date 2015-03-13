import Queue
import threading
import urllib2
import sys

NUM_THREADS = 30
INPUT_FILE = "../data/artist_list.txt"
OUTPUT_FILE = "../data/title_list.txt"
XAPP_TOKEN = "JvTPWe4WsQO-xqX6Bts49qjIwrgaUqjiFZJCEm7fMvVUEorceDZTN2s0lz_XhIw6oOQhs0l2XQWey0f0w1Mg6DodbYChO0uV3NgmyNuF2pglVlFnhaa5Wol48sWtLswT2uAWdfQpxTa4oN_MBJsI5bu1oyHRDdvzXJw1UUZPqn1-rquuP_iJPSBrCmbVhkh4GIjG0p8CFVnGwyx1tR56K966y7JIQ4HULxihqbD4AF8="
SEPARATOR = "%"

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

    def run(self):
        while True:
            artist_raw = self.artist_queue.get()
            name, url = artist_raw.split(SEPARATOR)

def write_header(file_obj):
    header = "title%sid%simage_url%sartist%sartist_url" % (SEPARATOR)
    file_obj.write(

def fetch_titles(artist_queue):
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
