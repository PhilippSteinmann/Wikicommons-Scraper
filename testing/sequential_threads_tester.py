import threading
import Queue
import random

queue1 = Queue.Queue()
queue2 = Queue.Queue()
for i in range(5):
    queue1.put(random.randint(1,10))

class ThreadOne(threading.Thread):
    def __init__(self, queue1, queue2):
        threading.Thread.__init__(self)
        self.queue1 = queue1
        self.queue2 = queue2

    def run(self):
        while True:
            number = self.queue1.get()
            self.queue2.put(number)
            self.queue1.task_done()
            print number

for i in range(5):
    thread = ThreadOne(queue1, queue2)
    thread.setDaemon(True)
    thread.start()

queue1.join()

print "done"
