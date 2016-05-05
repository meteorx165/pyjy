import threading

class Sem(object):

    def __init__(self, value):
        self.sem = threading.Semaphore(value)
        self.lock = threading.Lock()
        self.counter = value

    def acquire(self):
        with self.lock:
            self.sem.acquire()
            self.counter -= 1

    def release(self):
        with self.lock:
            self.sem.release()
            self.counter += 1

    def value(self):
        return self.counter
