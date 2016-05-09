# pyjy
A lightweight Python remote execution framework, can execute python function on remote cluster. It supports: load balance, variable broadcast.

TODO list:

1. Add async_execute() to PyjyClient/PyjyClusterClient;
2. Integrate with some external storages like Redis/HDFS, to avoid transferring huge input data on network repeatly.


<img src="https://raw.githubusercontent.com/meteorx165/pyjy/master/beethoven.jpg"></img>

# Usage
server:

    from server import PyjyServer

    srv = PyjyServer('0.0.0.0', 1234)
    srv.start()
    
client:

    from client import PyjyClient
    
    cli = PyjyClient('127.0.0.1', 1234)
    
    def add(x, y):
        return x + y
    
    # will print 3
    print cli.execute(add, args=[1, 2])

client use multi servers:

    from client import PyjyClusterClient
    
    cluster = PyjyClusterClient([('hostA', 1234), ('hostB', 1234), ('hostC', 1234)])

    def add(x, y):
        return x + y
    
    # thread-safe call
    print cli.execute(add, args=[1, 2]) # will print 3
    
broadcast variable to cluster:
    
    # broadcast variable to all nodes, and return a reference object
    ref = cluster.broadcast([1, 2, 3])
    def foo(a):
        return sum(a)
        
    # pass reference to args, instead of variable itself
    print cli.execute(foo, args=[ref]) # will print 6
