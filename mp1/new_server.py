import socket
import subprocess
import sys
import signal

# Create a socket object
s = socket.socket()
print("Socket successfully created")

# Reserve a port on your computer in our
# case it is 15000 but it can be anything
# if len(sys.argv) < 2:
#     print('Must provide port!')
#     sys.exit(-1)
# port = int(sys.argv[1])

port = 15000

# Next bind to the port
# we have not typed any ip in the ip field
# instead we have inputted an empty string
# this makes the server listen to requests
# coming from other computers on the network
s.bind(('', port))
print("socket binded to %s" %(port))

# Put the socket into listening mode
s.listen(5)
print("socket is listening")

# Handle SIGINT
def signal_handler(sig, frame):
        s.close()
        print("socket closed")
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

while True:
    out = ""
    # Establish connection with client.
    c, addr = s.accept()
    print('Got connection from', addr)

    # receive message
    msg = c.recv(1024).decode('utf-8').rstrip('\r\n')

    #if(msg == 'exit'):
    #   c.send(b'Exiting...\n')
    #   c.close()
    #   break

    try:
       out = subprocess.check_output(msg.split(' ')).decode('utf-8').splitlines()
    except subprocess.CalledProcessError as e:
        # If no match 
        if e.returncode == 1:
            c.send('1')
        # If command failed
        else:
            c.send('2')
        c.close()
        continue
    # If command executed successfully 
    c.send('0') 
    try:
        for line in out:
            c.send(line.encode('utf-8'))
            c.recv(1)
        c.send(b'\x00')
    except:
        print("Client disconnected")
        c.close()

    # Close the connection
    c.close()
