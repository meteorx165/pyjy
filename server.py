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
        server.broadcast_vars = {}
        server.serve_forever()


class WrappedThreadingTCPServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True
    request_queue_size = 1024


class PyjyHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        sock = self.connection
        req_type = struct.unpack('l', common.sock_recv(sock, 8))[0]

        if req_type == 1:
            try:
                self.server.worker_sem.acquire()
                self.do_execute(sock)
            finally:
                self.server.worker_sem.release()
        elif req_type == 2:
            self.do_stat(sock)
        elif req_type == 3:
            self.do_recv_broadcast(sock)
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

    def do_recv_broadcast(self, sock):
        req_header = common.sock_recv(sock, 16)
        key_data_len, value_data_len = struct.unpack('qq', req_header)
        key_data = common.sock_recv(sock, key_data_len)
        value_data = common.sock_recv(sock, value_data_len)
        key = cloudpickle.loads(key_data)
        value = cloudpickle.loads(value_data)
        self.server.broadcast_vars[key] = value
        
        result = {}
        result['success'] = True

        send_data = cloudpickle.dumps(result)
        send_data_len = len(send_data)
        sock.sendall(struct.pack('q', send_data_len))
        sock.sendall(send_data)

    def do_execute(self, sock):
        req_header = common.sock_recv(sock, 32)
        func_len, func_args_len, func_kwargs_len, bits = struct.unpack('qqqq', req_header)
        func = common.sock_recv(sock, func_len)
        func_args = common.sock_recv(sock, func_args_len)
        func_kwargs = common.sock_recv(sock, func_kwargs_len)
        is_fork = bits & 0x1

        func = cloudpickle.loads(func)
        func_args = cloudpickle.loads(func_args)
        func_kwargs = cloudpickle.loads(func_kwargs)

        # replace broadcast vars
        for i, e in enumerate(func_args):
            if type(e) == common.BroadcastVariableRef:
                func_args[i] = self.server.broadcast_vars[e.key]

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
                try:
                    ser_len = struct.unpack('q', os.read(rd, 8))[0]
                    ser_result = common.pipe_recv(rd, ser_len)
                finally:
                    os.close(rd)
                    os.close(wr)

        send_data = ser_result
        send_data_len = len(send_data)
        sock.sendall(struct.pack('q', send_data_len))
        sock.sendall(send_data)


if __name__ == '__main__':
    srv = PyjyServer('0.0.0.0', 8239)
    srv.start()
