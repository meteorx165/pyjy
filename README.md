# pyjy
Can execute python function on remote server/cluster with simple load balancer.

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

client use multi servers(multithreading is supported):

    from client import PyjyClusterClient
    
    cluster = PyjyClusterClient([('hostA', 1234), ('hostB', 1234), ('hostC', 1234)])

    def add(x, y):
        return x + y
    
    # will print 3
    print cli.execute(add, args=[1, 2])
    
broadcast variable to cluster:
    
    # broadcast variable to all nodes
    ref = cluster.broadcast([1, 2, 3])
    def foo(a):
        return sum(a)
        
    # will print 6
    print cli.execute(foo, args=[ref])
