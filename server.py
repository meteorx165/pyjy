import marshal
import struct
import types
import socket
import SocketServer
import threading

class PyjyServer(object):

    def __init__(self, host, port, worker_num=4):
        self.host = host
        self.port = port
        self.worker_num = worker_num

    def start(self):
        server = WrappedThreadingTCPServer((self.host, self.port), PyjyHandler)
        server.worker_sem = threading.Semaphore(self.worker_num)
        server.serve_forever()


class WrappedThreadingTCPServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True
    request_queue_size = 1024


class PyjyHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        sock = self.connection
        req_type = struct.unpack('l', sock.recv(8))[0]

        if req_type == 1:
            try:
                self.server.worker_sem.acquire()
                self.do_execute(sock)
            finally:
                self.server.worker_sem.release()
        else:
            raise Exception('unrecognized req type: %d' % (req_type, ))

    def do_execute(self, sock):
        req_header = sock.recv(32)
        func_code_len, func_closure_len, func_args_len, func_kwargs_len = struct.unpack('qqqq', req_header)
        func_code = sock.recv(func_code_len)
        func_closure = sock.recv(func_closure_len)
        func_args = sock.recv(func_args_len)
        func_kwargs = sock.recv(func_kwargs_len)

        func = types.FunctionType(marshal.loads(func_code), {}, "fff", closure=marshal.loads(func_closure))
        result = func(*marshal.loads(func_args), **marshal.loads(func_kwargs))

        send_data = marshal.dumps(result)
        send_data_len = len(send_data)
        sock.sendall(struct.pack('q', send_data_len))
        sock.sendall(send_data)
