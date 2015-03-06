import Queue
import threading
import urllib2
from bs4 import BeautifulSoup

NUM_THREADS = 30
OUTPUT_URL = "artist_list.txt"

def get_link_for_each_letter():
    letter_links = {}
    base_url = "http://www.artsy.net/artists/artists-starting-with-"
    letter_a = ord("a")

    for i in range(26):
        letter = chr(letter_a + i)
        url = base_url + letter
        letter_links[letter] = url

    return letter_links

class FetchPageLength(threading.Thread):
    def __init__(self, letter_queue, lock, page_lengths):
        threading.Thread.__init__(self)
        self.letter_queue = letter_queue
        self.lock = lock
        self.page_lengths = page_lengths

    def find_page_length(self, soup):
        paginator_element = soup.select("a.paginator-last")
        if (len(paginator_element) > 0):
            paginator_element = paginator_element[0]
            last_page = paginator_element["data-value"]
            return int(last_page)
        else:
            return 1

    def run(self):
        while True:
            letter_link = self.letter_queue.get()
            letter = letter_link[-1]

            try:
                html = urllib2.urlopen(letter_link).read()
            except:
                print letter_link
                self.letter_queue.put(letter_link)
                self.letter_queue.task_done()
                continue

            soup = BeautifulSoup(html)

            page_length = self.find_page_length(soup)

            self.lock.acquire()
            self.page_lengths[letter] = page_length
            self.lock.release()
            self.letter_queue.task_done()


def get_page_lengths(letter_links):
    letter_queue = Queue.Queue()
    for letter_link in letter_links.values():
        letter_queue.put(letter_link)

    lock = threading.Lock()
    page_lengths = {}

    for i in range(NUM_THREADS):
        new_thread = FetchPageLength(letter_queue, lock, page_lengths)
        new_thread.setDaemon(True)
        new_thread.start()

    letter_queue.join()
    
    return page_lengths

def generate_all_pages(letter_links, page_lengths):
    all_pages = []
    pagination_base = "?page="

    for i in range(25):
        letter = chr(ord("a") + i)
        base_url = letter_links[letter]
        page_length = page_lengths[letter]

        for page in range(1, page_length + 1):
            url = base_url + pagination_base + str(page)
            all_pages.append(url)

    return all_pages


class FetchArtists(threading.Thread):
    def __init__(self, page_queue, lock, artists):
        threading.Thread.__init__(self)
        self.page_queue = page_queue
        self.lock = lock
        self.artists = artists

    def find_artists(self, soup):
        artists_in_page = []

        artist_list = soup.select(".artists-columns a.highlight-link")
        for artist_elem in artist_list:
            artist_name = unicode(artist_elem.string)
            artist_url = artist_elem["href"]
            artist = [artist_name, artist_url]
            artists_in_page.append(artist)

        return artists_in_page

    def run(self):
        while True:
            page_url = self.page_queue.get()

            try:
                html = urllib2.urlopen(page_url).read()
            except:
                #print page_url
                self.page_queue.put(page_url)
                self.page_queue.task_done()
                continue

            soup = BeautifulSoup(html)

            artists_in_page = self.find_artists(soup)

            self.lock.acquire()
            self.artists += artists_in_page
            self.lock.release()
            self.page_queue.task_done()


def get_artists(all_pages):
    page_queue = Queue.Queue()
    for page in all_pages:
        page_queue.put(page)

    artists = []
    lock = threading.Lock()

    for i in range(NUM_THREADS):
        new_thread = FetchArtists(page_queue, lock, artists)
        new_thread.setDaemon(True)
        new_thread.start()

    page_queue.join()
    
    return artists

def write_artists_to_file(artists):
    file_obj = open(OUTPUT_URL, "w+")

    header = "name,url\n"

    file_obj.write(header)

    for artist in artists:
        artist_string = ",".join(artist)
        artist_string = artist_string.encode("utf-8") + "\n"
        file_obj.write(artist_string)

    file_obj.close()

def main():
    print "Getting links for A-Z..."
    letter_links = get_link_for_each_letter()

    print "Getting page lengths of A-Z..."
    page_lengths = get_page_lengths(letter_links)
    all_pages = generate_all_pages(letter_links, page_lengths)

    print "Getting artist names from %d pages..." % (len(all_pages))
    artists = get_artists(all_pages)

    print "Writing %d names to file..." % (len(artists))
    write_artists_to_file(artists)
    
    print "Done."


if __name__ == "__main__":
    main()
