import marshal
import socket
import struct

class PyjyClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def execute(self, func, args=None, kwargs={}):
        func_code = marshal.dumps(func.func_code)
        func_closure = marshal.dumps(func.func_closure)
        func_args = marshal.dumps(args)
        func_kwargs = marshal.dumps(kwargs)

        send_data = struct.pack('qqqqq', 1, len(func_code), len(func_closure), len(func_args), len(func_kwargs)) \
                + func_code + func_closure + func_args + func_kwargs

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

        return marshal.loads(recv_data)
