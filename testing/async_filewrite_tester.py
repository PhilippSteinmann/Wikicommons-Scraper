import csv
import threading
import random

class WriteToCSV(threading.Thread):
    def __init__(self, lock):
        threading.Thread.__init__(self)
        self.lock = lock

    def run(self):
        print "IN THREAD"
        a = random.randint(1, 10)
        b = random.randint(1, 10)
        c = random.randint(1, 10)
        dictionary = {"a":a, "b":b, "c":c}

        self.lock.acquire()
        csv_writer.writerow(dictionary)
        self.lock.release()


# Create lock for file
file_lock = threading.Lock()

# Open CSV file for appending
file_obj = open("metadata.csv", "a+")

# Needed to convert dictionary -> CSV
csv_writer = csv.DictWriter(file_obj, ["a", "b", "c"])
csv_writer.writeheader()

for i in range(10):
    thread = WriteToCSV(file_lock)
    thread.setDaemon(True)
    thread.start()
