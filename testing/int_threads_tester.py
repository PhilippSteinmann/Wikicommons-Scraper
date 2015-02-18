import threading

integer = 0
INTEGER = 0
list_ = [0]

class MyThread(threading.Thread):
    def __init__(self, list_, lock):
        threading.Thread.__init__(self)
        self.list_ = list_
        self.lock = lock

    def run(self):
        while True:
            #print INTEGER
            #print integer
            #integer += 1
            self.lock.acquire()
            self.list_[0] += 1
            self.lock.release()

            print self.list_[0]

lock = threading.Lock()

for i in range(5):
    t = MyThread(list_, lock)
    t.setDaemon(True)
    t.run()
