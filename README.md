# pyjy
a python server can execute function(closure) remotely.

# Usage
server:

    from server import PyjyServer

    srv = PyjyServer('0.0.0.0', 1234)
    srv.start()
    
client:

    from client import PyjyClient
    
    cli = PyjyClient('0.0.0.0', 1234)
    
    def add(x, y):
        return x + y
    
    # will print 3
    print cli.execute(add, args=[1, 2])
