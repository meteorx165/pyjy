import socket
import struct
import datetime
import threading
import time
import random
import cloudpickle

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
                time.sleep(5)

        update_th = threading.Thread(target=_update_stat, args=(self, ))
        update_th.setDaemon(True)
        update_th.start()

    def update_stat(self):
        for cli in self.clients:
            try:
                stat = cli.stat()
                cli.total_slot = stat['total_slot']
                cli.free_slot = stat['free_slot']
                cli.last_ban = None
            except:
                cli.last_ban = datetime.datetime.now()

    def print_stat(self):
        for cli in self.clients:
            print '%s:%s %d/%d' % (cli.host, cli.port, cli.free_slot, cli.total_slot), 
        print

    def execute(self, func, args=None, kwargs={}, max_retry=10, sleep=1.0):
        for i in xrange(max_retry):
            try:
                return self.execute_oneshot(func, args, kwargs)
            except:
                time.sleep(1)
                pass
        return self.execute_oneshot(func, args, kwargs)

    def execute_oneshot(self, func, args=None, kwargs={}):
        avail_clients = [cli for cli in self.clients if cli.last_ban is None]
        avail_clients.sort(key = lambda cli: -cli.free_slot)
        choosed = avail_clients[:int(len(avail_clients) * 0.2 + 1.0)]
        random.shuffle(choosed)
        cli = choosed[0]
        try:
            cli.free_slot -= 1
            return cli.execute(func, args, kwargs)
        except Exception as e:
            cli.last_ban = datetime.datetime.now()
            raise
        finally:
            cli.free_slot = min(cli.free_slot + 1, cli.total_slot)

class PyjyClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def execute(self, func, args=None, kwargs={}):
        func = cloudpickle.dumps(func)
        func_args = cloudpickle.dumps(args)
        func_kwargs = cloudpickle.dumps(kwargs)

        send_data = struct.pack('qqqq', 1, len(func), len(func_args), len(func_kwargs)) \
                + func + func_args + func_kwargs

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        sock.sendall(send_data)

        recv_header = sock.recv(8)
        recv_data_len = struct.unpack('q', recv_header)[0]
        recv_buf = []
        read_len = 0
        while read_len < recv_data_len:
            data = sock.recv(1024)
            if not data:
                break
            read_len += len(data)
            recv_buf.append(data)
        sock.close()
        recv_data = ''.join(recv_buf)

        return cloudpickle.loads(recv_data)
    
    def stat(self):
        send_data = struct.pack('q', 2)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        sock.sendall(send_data)

        recv_header = sock.recv(8)
        recv_data_len = struct.unpack('q', recv_header)[0]
        recv_buf = []
        read_len = 0
        while read_len < recv_data_len:
            data = sock.recv(1024)
            if not data:
                break
            read_len += len(data)
            recv_buf.append(data)
        sock.close()
        recv_data = ''.join(recv_buf)

        return cloudpickle.loads(recv_data)
