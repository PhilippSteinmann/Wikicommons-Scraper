import Queue
import threading
import time

class MyThread(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            number = self.queue.get()
            print number
            self.queue.task_done()

            self.queue.put(number + 1)
            time.sleep(0.3)


queue = Queue.Queue()
queue.put(1)

for i in range(5):
    t = MyThread(queue)
    t.setDaemon(True)
    t.run()

queue.join()
