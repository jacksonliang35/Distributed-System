import socket
import sys
import threading as td
from multiprocessing import Pool as ThreadPool

num_machine = 0

def grep_from_mesg(argv):

    i = argv[0]
    pattern = argv[1]
    file_name = argv[2]
    
    # Connect to server
    try:
        s = socket.create_connection(('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(i), 15000))
    except:
        #If connect failed
        return -1
    
    # Send command
    user_cmd = 'grep {} {}{}.log'.format(pattern,file_name, i)
    cmd =  'grep -n {} {}{}.log'.format(pattern,file_name, i)
    s.send(cmd.encode('utf-8'))
    print('M{}: command sent => \'{}\''.format(i,user_cmd))

    # Fetch status
    status = s.recv(1)
    # If no match
    if status == '1':
        return 0
    # If command failed
    if status == '2':
        return -2

    # Line counter
    cnt = 0
    while True:
        # Receive result
        rec = s.recv(256).decode('utf-8')

        s.send(b'\x01')
        # No more message available
        if rec == '\x00':
            break
        print('M%s: L%s' % (i, rec))
        cnt += 1
    
    return cnt

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('usage: python new_client.py [#machines] [pattern] [file_name]')
        sys.exit(-1)
    num_machine = int(sys.argv[1])
    pattern = sys.argv[2]
    file_name = sys.argv[3]
    pool = ThreadPool(num_machine) 
    results = pool.map(grep_from_mesg, zip(range(1, num_machine+1), [pattern]*num_machine, [file_name]*num_machine))
    print('-----------')
    for i, res in enumerate(results):
        if res == -1:
            print('M%s: connection failed'% (i+1))
        elif res == -2:
            print('M%s: command execution failed'% (i+1))
        else:
            print('M%s: total line count %s' % ((i+1), res))
    print('Total Line Count: {}'.format(sum([num for num in results if num >=0])))
   
