#!/usr/bin/env python

import struct
import types
import socket
import SocketServer
import subprocess
import threading
import common
import cloudpickle
import os

class PyjyServer(object):

    def __init__(self, host, port, worker_num=0):
        self.host = host
        self.port = port
        self.worker_num = worker_num
        if self.worker_num <= 0:
            self.worker_num = int(subprocess.check_output(['/bin/sh', '-c', "grep -c ^processor /proc/cpuinfo"]).strip())

    def start(self):
        server = WrappedThreadingTCPServer((self.host, self.port), PyjyHandler)
        server.worker_sem = common.Sem(self.worker_num)
        server.worker_num = self.worker_num
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
        elif req_type == 2:
            self.do_stat(sock)
        else:
            raise Exception('unrecognized req type: %d' % (req_type, ))

    def do_stat(self, sock):
        free_slot_num = self.server.worker_sem.value()
        result = {}
        result['free_slot'] = free_slot_num
        result['total_slot'] = self.server.worker_num

        send_data = cloudpickle.dumps(result)
        send_data_len = len(send_data)
        sock.sendall(struct.pack('q', send_data_len))
        sock.sendall(send_data)

    def do_execute(self, sock):
        req_header = sock.recv(32)
        func_len, func_args_len, func_kwargs_len, bits = struct.unpack('qqqq', req_header)
        func = sock.recv(func_len)
        func_args = sock.recv(func_args_len)
        func_kwargs = sock.recv(func_kwargs_len)
        is_fork = bits & 0x1

        func = cloudpickle.loads(func)
        func_args = cloudpickle.loads(func_args)
        func_kwargs = cloudpickle.loads(func_kwargs)

        if not is_fork:
            result = func(*func_args, **func_kwargs)
            ser_result = cloudpickle.dumps(result)
        else:
            rd, wr = os.pipe()
            pid = os.fork()
            if pid == 0: # child
                result = func(*func_args, **func_kwargs)
                ser_result = cloudpickle.dumps(result)
                os.write(wr, struct.pack('q', len(ser_result)))
                i = 0
                while i < len(ser_result):
                    i += os.write(wr, ser_result[i:])
                # avoid exception
                os._exit(0)
            else: # parent
                ser_len = struct.unpack('q', os.read(rd, 8))[0]
                buf = []
                read_len = 0
                while read_len < ser_len:
                    ret = os.read(rd, 1024)
                    if not ret:
                        break
                    buf.append(ret)
                    read_len += len(ret)
                ser_result = ''.join(buf)
                os.close(rd)
                os.close(wr)

        send_data = ser_result
        send_data_len = len(send_data)
        sock.sendall(struct.pack('q', send_data_len))
        sock.sendall(send_data)


if __name__ == '__main__':
    srv = PyjyServer('0.0.0.0', 8239)
    srv.start()
