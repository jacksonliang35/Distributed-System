import socket
import sys
import datetime
import time
import copy
import threading
import signal
import os
import logging
import random

INTERVAL = 1.0      ## ping interval, in second
ITDN = 1            ## introducer vm number
PING = b'\x00'      ## ping message
MEMBERSHIP = {}     ## membership list. One member: (timestamp, incarnation, alive)
MBSLOCK = threading.Lock()
TOLIST = []         ## timeout list
RPORT = 12345       ## receiver port
IPORT = 12445       ## introduer port
VM = 0              ## vm number
LEAVE = False       ## want to leave
STATUS = {1:"alive", 2:"failed", 0:"left"} ## Status dictionary
TIMESTAMP = 0       ## timestamp for current joined process
VM_NUM = 11         ## total number of vm
VM_NUM_SIZE = 2     ## size of VM_NUM 
INCARNATION_SIZE = 4  ## size of INCARNATION
TIMESTAMP_SIZE = 14   ## size of TIMESTAMP
STATUS_SIZE = 2       ## size of STATUS
CURRENT_IP = socket.gethostbyname(socket.gethostname())      ##Current ip address
## Initalize the logging system
logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(message)s',level=logging.INFO)

## Print current membership list
def print_membership():
    MBSLOCK.acquire()
    for i in range(1,11):
        member = MEMBERSHIP[i]
        if member[0] != 0:
            print("{}:{} {} {}".format(i, member[0], STATUS[member[2]],member[1]))
    MBSLOCK.release()

## Emulate crashing the process
def sigint_handler(sig, frame):
    os._exit(0)


## High-level abstraction of sending messages
def send_mesg(addr, port, msg):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(msg, (addr,port))
        return 0
    except:
        return -1

## Encoder for membership list
def mbs_encoder():
    mesg = format(VM, '0'+str(VM_NUM_SIZE)+'d') + "|"  # id of current vm
    MBSLOCK.acquire()
    for i in range(1, 11):
        mesg = mesg + format(MEMBERSHIP[i][0], '0'+str(TIMESTAMP_SIZE)+'d') + ","+ format(MEMBERSHIP[i][1], '0'+str(INCARNATION_SIZE)+'d')+ "," + str(MEMBERSHIP[i][2])+ "|"
    MBSLOCK.release()
    return(mesg.encode('utf-8'))

## Decode for membership list
def mbs_decoder(message, addr):
    ret_dict = {}
    mesg = message.decode('utf-8').rstrip('|').split('|')
    
    ## error checking
    if len(mesg) != 11:
        print("Cannot decode message!")
        return -1
    ## No time-out
    TOLIST[int(mesg[0]) - 1] = False
    ## Decode membership list
    for i in range(1, 11):
        mesgs = mesg[i].split(",")
        ret_dict[i] = (int(mesgs[0]),int(mesgs[1]), int(mesgs[2]))
    

    ## Update current membership list
    ## acquire membership list lock
    MBSLOCK.acquire()
    for i in range(1, 11):
        if ret_dict[i][1] > MEMBERSHIP[i][1] or (ret_dict[i][1] == MEMBERSHIP[i][1] and ret_dict[i][2] == 2 and MEMBERSHIP[i][2] == 1):
            MEMBERSHIP[i] = ret_dict[i]
            status_change = ""
            if(MEMBERSHIP[i][2] == 1):
                status_change = "joined"
            else: 
                status_change = STATUS[MEMBERSHIP[i][2]]
            if(i != VM):
                logging.info("{}:{} {}".format(i, MEMBERSHIP[i][0], status_change))
    MBSLOCK.release()
    if MEMBERSHIP[VM][2] == 2:
        MBSLOCK.acquire()
        MEMBERSHIP[VM] = (MEMBERSHIP[VM][0], MEMBERSHIP[VM][1]+1, 1)
        MBSLOCK.release()
        print(send_mesg(addr, RPORT, mbs_encoder()))
        logging.info("{}:{} refuse failure detection".format(VM, MEMBERSHIP[VM][1]))
        

    ## release lock
   
    return 0

## Find the ping-to list of a node
def ping_to_list(vm, mbslist):
    ret = []
    cnt = 0
    for i in range(9):
        if mbslist[(i + vm) % 10 + 1][2] == 1:
            ret.append((i + vm) % 10 + 1)
            cnt += 1
            if cnt == 4:
                break
    return ret

## Find the ping-from list of a node
def ping_from_list(vm, mbslist):
    ret = []
    cnt = 0
    for i in range(1, 10):
        if mbslist[(vm + 9 - i) % 10 + 1][2] == 1:
            ret.append((vm + 9 - i) % 10 + 1)
            cnt += 1
            if cnt == 4:
                break
    return ret

## When the process leave, update the membership list 
def leave():
    global LEAVE
    logging.info("{}:{} (self) left".format(VM,TIMESTAMP))
    MBSLOCK.acquire()
    MEMBERSHIP[VM] = (MEMBERSHIP[VM][0],MEMBERSHIP[VM][1]+1, 0)
    mbslist = MEMBERSHIP.copy()
    plist = ping_to_list(VM, mbslist)
    MBSLOCK.release()
    
    for v in plist:
        send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(v), RPORT, mbs_encoder())
    MBSLOCK.acquire()
    LEAVE = True
    MBSLOCK.release()

## Join a group
def join_group():
    global TIMESTAMP
    TIMESTAMP = int('{:%Y%m%d%H%M%S}'.format(datetime.datetime.now()))
    
    if VM == ITDN:
        ## Introducer initialization
        ## update membership list
        MBSLOCK.acquire()
        MEMBERSHIP[1] = (TIMESTAMP, MEMBERSHIP[1][1] + 1, 1)
        MBSLOCK.release()
        logging.info("{}:{} initalized as introducer".format(VM,TIMESTAMP))
        return -2
    logging.info("{}:{} initalized".format(VM,TIMESTAMP))
    if send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(ITDN), IPORT, "{}:{}".format(VM,TIMESTAMP).encode('utf-8')) < 0:
        print("Failed to join group!")
        return -1
    MEMBERSHIP[VM] = (TIMESTAMP,MEMBERSHIP[VM][1] + 1, 1)
    logging.info("{}:{} joined the group".format(VM,TIMESTAMP))
    ## Success
    return 0

def ping_once(vm):
    TOLIST[vm - 1] = True
    pingged_ip = socket.gethostbyname('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm).strip())
    logging.info("{}:{} pinged to {}".format(VM,TIMESTAMP,pingged_ip))
    if -1 == send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), RPORT, PING):
        ## vm failed
        MBSLOCK.acquire()
        if(MEMBERSHIP[vm][2] == 1):
            MEMBERSHIP[vm] = (MEMBERSHIP[vm][0],MEMBERSHIP[vm][1], 2)
        MBSLOCK.release()
        logging.info("{}:{} failed because pinging failed".format(vm, MEMBERSHIP[vm][0]))
        return
    ## Timeout
    time.sleep(INTERVAL / 2)
    if TOLIST[vm - 1]:
        MBSLOCK.acquire()
        if(MEMBERSHIP[vm][2] == 1):
            MEMBERSHIP[vm] = (MEMBERSHIP[vm][0],MEMBERSHIP[vm][1], 2)
        MBSLOCK.release()
        logging.info("{}:{} failed because ACK timeout".format(vm, MEMBERSHIP[vm][0]))
    return


def initialize_receiver():
    ## Initialize socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ## Allow computer from other network to access the port
    s.bind(('', RPORT))
    
    while not LEAVE:
       
        ## Receive and decode message if necessary
        msg, addr = s.recvfrom(VM_NUM*(TIMESTAMP_SIZE+INCARNATION_SIZE+STATUS_SIZE+3)+ VM_NUM_SIZE+1)
        if msg == PING:
            ## Send back membership list
            # ran = random.randint(1,100)
            # if ran <= 30:
            #     continue
            send_mesg(addr[0], RPORT, mbs_encoder())
            logging.info("{}:{} ACKed to {}".format(VM,TIMESTAMP, addr[0]))
        else:
            mbs_decoder(msg,addr[0])

def initialize_pinger():
    if join_group() == -1:
        sys.exit(-1)
    starttime = time.time()
    while not LEAVE:
        MBSLOCK.acquire()
        mbslist = MEMBERSHIP.copy()
        MBSLOCK.release()
        plist = ping_to_list(VM, mbslist)
        for v in plist:
            threading.Thread(target=ping_once, args=(v,)).start()

        #print_membership()
        ## Repeat pinging every INTERVAL seconds
        time.sleep(INTERVAL - ((time.time() - starttime) % INTERVAL))

def initialize_introducer():
    ## Introducer socket
    itds = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ## bind to introducer port
    itds.bind(('', IPORT))
    

    ## Constantly introduce new node
    while not LEAVE:
        msg, addr = itds.recvfrom(TIMESTAMP_SIZE+VM_NUM_SIZE+1)
        msgs = msg.decode('utf-8').split(':')
        nn = int(msgs[0])
        timestamp = int(msgs[1])
        if nn < 1 or nn > 10:
            print("Invalid VM number")
            continue

        MBSLOCK.acquire()
        MEMBERSHIP[nn] = (timestamp, MEMBERSHIP[nn][1] + 1, 1)
        MBSLOCK.release()
        send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(nn), RPORT, mbs_encoder())

if __name__ == '__main__':
    ## Initialize vm number
    if len(sys.argv) < 2:
        print("Need enter vm number!")
        sys.exit(-1)
    VM = int(sys.argv[1])
    assert(VM > 0 and VM < 11)

    ## Initialize membership list
    for i in range(1, 11):
        MEMBERSHIP[i] = (0, 0, 0)  ## (timestamp, incarnation, alive)

    ## Initialize timeout list
    TOLIST = [False] * 10

    ## Catch signals
    signal.signal(signal.SIGINT, sigint_handler)

    ## Start
    rec_tr = threading.Thread(target=initialize_receiver)
    rec_tr.start()
    if VM == ITDN:
        ind_tr = threading.Thread(target=initialize_introducer)
        ind_tr.start()
    pin_tr = threading.Thread(target=initialize_pinger)
    pin_tr.start()
    
   
    while(1):
        print('==========')
        print('Type \'c\' to print out self-id')
        print('Type \'p\' to print out the membership list')
        print('Type \'l\' to leave')
        print('')
        cmd = raw_input()
        if(cmd == 'p'):
            print('')
            print_membership()
            print('')
        elif(cmd == 'l'):
            leave()
            os._exit(0)
        elif(cmd == 'c'):
            print('')
            print('{}:{}'.format(VM, TIMESTAMP))
            print('')
    
    rec_tr.join()
    ind_tr.join()
    if ind_tr != None:
        ind_tr.join()
    pin_tr.join()

