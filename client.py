import socket
import struct
import datetime
import threading
import time
import random
import cloudpickle
import common

class PyjyClusterClient(object):

    def __init__(self, host_and_ports):
        clients = []
        for host, port in host_and_ports:
            cli = PyjyClient(host, port)
            cli.last_ban = None # last banned timestamp
            clients.append(cli)
        self.clients = clients
        self.update_stat()

        def _update_stat(cluster):
            while True:
                cluster.update_stat()
                time.sleep(1)

        update_th = threading.Thread(target=_update_stat, args=(self, ))
        update_th.setDaemon(True)
        update_th.start()

    def get_stat(self):
        free, total = 0, 0
        for cli in self.clients:
            free += cli.free_slot
            total += cli.total_slot
        return free, total

    def is_busy(self):
        free, total = self.get_stat()
        return free >= total * 0.9

    def broadcast(self, value):
        var = common.BroadcastVariable.create(value)
        for cli in self.clients:
            cli.send_broadcast(var)
        return var.ref()

    def update_stat(self):
        for cli in self.clients:
            try:
                stat = cli.stat()
                cli.total_slot = stat['total_slot']
                cli.free_slot = stat['free_slot']
                cli.last_ban = None
            except:
                cli.total_slot = 0
                cli.free_slot = 0
                cli.last_ban = datetime.datetime.now()

    def print_stat(self):
        print '=' * 10, 'Stat', '=' * 10
        for cli in self.clients:
            print '%s:%s %d/%d' % (cli.host, cli.port, cli.free_slot, cli.total_slot)

    def execute(self, func, args=None, kwargs={}, is_fork=False, max_retry=10, sleep=1.0):
        for i in xrange(max_retry):
            try:
                return self.execute_oneshot(func, args, kwargs, is_fork)
            except:
                time.sleep(1)
                pass
        return self.execute_oneshot(func, args, kwargs)

    def execute_oneshot(self, func, args=None, kwargs={}, is_fork=False):
        avail_clients = [cli for cli in self.clients if cli.last_ban is None]
        avail_clients.sort(key = lambda cli: -cli.free_slot)
        choosed = avail_clients[:int(len(avail_clients) * 0.2 + 1.0)]
        random.shuffle(choosed)
        cli = choosed[0]
        try:
            cli.free_slot -= 1
            return cli.execute(func, args, kwargs, is_fork)
        except Exception as e:
            cli.last_ban = datetime.datetime.now()
            raise
        finally:
            cli.free_slot = min(cli.free_slot + 1, cli.total_slot)

class PyjyClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def execute(self, func, args=None, kwargs={}, is_fork=False):
        func = cloudpickle.dumps(func)
        func_args = cloudpickle.dumps(args)
        func_kwargs = cloudpickle.dumps(kwargs)
        bits = (0x1 * is_fork)

        send_data = struct.pack('qqqqq', 1, len(func), len(func_args), len(func_kwargs), bits) \
                + func + func_args + func_kwargs

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        sock.sendall(send_data)

        recv_header = sock.recv(8)
        recv_data_len = struct.unpack('q', recv_header)[0]
        recv_data = common.sock_recv(sock, recv_data_len)
        return cloudpickle.loads(recv_data)
    
    def stat(self):
        send_data = struct.pack('q', 2)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        sock.sendall(send_data)

        recv_header = sock.recv(8)
        recv_data_len = struct.unpack('q', recv_header)[0]
        recv_data = common.sock_recv(sock, recv_data_len)
        return cloudpickle.loads(recv_data)
    
    def broadcast(self, value):
        var = common.BroadcastVariable.create(value)
        self.send_broadcast(var)
        return var.ref()
    
    def send_broadcast(self, var):
        key_data = cloudpickle.dumps(var.key)
        value_data = cloudpickle.dumps(var.value)
        send_data = struct.pack('qqq', 3, len(key_data), len(value_data)) \
                + key_data + value_data

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        sock.sendall(send_data)

        recv_header = sock.recv(8)
        recv_data_len = struct.unpack('q', recv_header)[0]
        recv_data = common.sock_recv(sock, recv_data_len)
        return cloudpickle.loads(recv_data)
