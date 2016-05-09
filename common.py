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


def sock_recv(sock, length):
    buf = []
    rd = 0
    while rd < length:
        s = sock.recv(length - rd)
        if len(s) == 0:
            break
        buf.append(s)
        rd += len(s)
    return ''.join(buf)
