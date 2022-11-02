import socket
import sys
import datetime
import time
import copy
import threading
import signal
import os
import logging
import pickle
import time

######### MP3 file system global constants #########
SDFS_DIR = 'sdfs_files/'    ## File system directory
FILE_VER = {}       ## Format: key -- sdfs_name, value -- version
NAMESPACE = {}      ## Format: key -- sdfs_name, value -- list of shared node
INV_NAMES = {}      ## Format: key -- node, value -- shared sdfs_names
MAX_VERSIONS = 5    ## Maximum version control number
FDATA_PORT = 12545  ## File Port
FCMD_PORT = 12645   ## Command Port
OTHER_PORT = 12745  ## Other Port (versions_sender, get_version, sdfs_file_exist)
FILELOCK = threading.Lock()
MAX_CMD_LEN = 512
REPLICA_NUM = 3     ## Number of replicas
W = 4               ## Writer number
R = 1               ## Reader number

######### MP2 membership list global constants ########
INTERVAL = 1.0      ## ping interval, in second
ITDN = 10           ## introducer vm number
PING = b'\x00'      ## ping message
MEMBERSHIP = {}     ## membership list. One member: (timestamp, incarnation, alive)
MBSLOCK = threading.Lock()
TOLIST = []         ## timeout list for membership
RPORT = 12345       ## receiver port
IPORT = 12445       ## introduer port
VM = 0              ## vm number
LEAVE = False       ## flag to leave
STATUS = {1:"alive", 2:"failed", 0:"left"} ## Status dictionary
TIMESTAMP = 0       ## timestamp for current joined process
VM_NUM = 11         ## total number of vm
VM_NUM_SIZE = 2     ## size of VM_NUM
INCARNATION_SIZE = 4  ## size of INCARNATION
TIMESTAMP_SIZE = 14   ## size of TIMESTAMP
STATUS_SIZE = 2       ## size of STATUS
CURRENT_IP = socket.gethostbyname(socket.gethostname())     ## Current ip address

## Initalize the logging system
logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(message)s',level=logging.INFO)

########## Basic Functions from MP2 ###########
## Print current membership list
def print_all():
    MBSLOCK.acquire()
    for i in range(1,11):
        member = MEMBERSHIP[i]
        if member[0] != 0:
            print("{}:{} {} {}".format(i, member[0], STATUS[member[2]],member[1]))
    MBSLOCK.release()
    FILELOCK.acquire()
    for i in range(1,10):
        print(INV_NAMES[i])
    print(NAMESPACE)
    FILELOCK.release()

## Emulate crashing the process
# def sigint_handler(sig, frame):
#     os._exit(0)

## High-level abstraction of sending messages
def send_mesg(addr, port, msg):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(msg, (addr, port))
        s.close()
        return 0
    except:
        return -1

## Encoder for membership list
def mbs_encoder():
    mesg = format(VM, '0' + str(VM_NUM_SIZE) + 'd') + "|"  # id of current vm
    MBSLOCK.acquire()
    for i in range(1, 11):
        mesg = mesg + format(MEMBERSHIP[i][0], '0'+str(TIMESTAMP_SIZE)+'d') + ","+ format(MEMBERSHIP[i][1], '0'+str(INCARNATION_SIZE)+'d')+ "," + str(MEMBERSHIP[i][2])+ "|"
    MBSLOCK.release()
    return mesg.encode('utf-8')

## Decode for membership list
def mbs_decoder(message, addr):
    ret_dict = {}
    mesg = message.decode('utf-8').rstrip('|').split('|')

    ## error checking
    if len(mesg) != 11:
        print("Cannot decode message!")
        return -1
    ## Not timed out
    TOLIST[int(mesg[0]) - 1] = False
    ## Decode membership list
    for i in range(1, 11):
        mesgs = mesg[i].split(",")
        ret_dict[i] = (int(mesgs[0]),int(mesgs[1]), int(mesgs[2]))

    ## Update current membership list

    MBSLOCK.acquire()
    for i in range(1, 11):
        change = False

        if ret_dict[i][1] > MEMBERSHIP[i][1] or (ret_dict[i][1] == MEMBERSHIP[i][1] and ret_dict[i][2] == 2 and MEMBERSHIP[i][2] == 1):
            # New join case or (Failure/Left case)
            MEMBERSHIP[i] = ret_dict[i]
            change = True
        # else:
        #     # replicate a membership list
        #     ret_dict[i] = MEMBERSHIP[i]


        if change:
            status_change = ""
            if(ret_dict[i][2] == 1):
                status_change = "joined"
            else:
                status_change = STATUS[ret_dict[i][2]]
            if(i != VM):
                logging.info("{}:{} {}".format(i, ret_dict[i][0], status_change))


    ## Reincarnate
    if MEMBERSHIP[VM][2] == 2:
        # Reincarnate
        MEMBERSHIP[VM] = (MEMBERSHIP[VM][0], MEMBERSHIP[VM][1]+1, 1)
        MBSLOCK.release()
        send_mesg(addr, RPORT, mbs_encoder())
        logging.info("{}:{} refuse failure detection".format(VM, MEMBERSHIP[VM][1]))
    else:
        MBSLOCK.release()

    ## Success
    return 0

## Find the ping-to list of a node
def ping_to_list(vm, mbslist, N):
    ret = []
    cnt = 0
    for i in range(N-1):
        if mbslist[(i + vm) % N + 1][2] == 1:
            ret.append((i + vm) % N + 1)
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
    MBSLOCK.release()

    plist = ping_to_list(VM, mbslist, 10)
    for v in plist:
        send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(v), RPORT, mbs_encoder())
    # Wait for time out

    MBSLOCK.acquire()
    LEAVE = True
    MBSLOCK.release()
    return 0

## Join a group
def join_group():
    global TIMESTAMP
    TIMESTAMP = int('{:%Y%m%d%H%M%S}'.format(datetime.datetime.now()))

    if VM == ITDN:
        ## Introducer initialization
        ## update membership list
        MBSLOCK.acquire()
        MEMBERSHIP[VM] = (TIMESTAMP, MEMBERSHIP[VM][1] + 1, 1)
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
        return 0
    ## Timeout
    time.sleep(INTERVAL / 4)
    if TOLIST[vm - 1]:
        MBSLOCK.acquire()
        ret_dict = MEMBERSHIP.copy()
        if(MEMBERSHIP[vm][2] == 1):
            MEMBERSHIP[vm] = (MEMBERSHIP[vm][0],MEMBERSHIP[vm][1], 2)
        MBSLOCK.release()
        # # Failure
        # names_change = {}
        # FILELOCK.acquire()
        # try:
        #     for files in INV_NAMES[vm]:
        #         NAMESPACE[files].remove(vm)
        #         names_change[files] = NAMESPACE[files]
        #     INV_NAMES[vm] = []
        # except Exception as e:
        #     print("Exception on pinging")
        #     print(e)
        # FILELOCK.release()
        # # Replicate
        # for files in names_change:
        #     if VM == min(names_change[files]):
        #         replicate(files, ret_dict, names_change[files])
        logging.info("{}:{} failed because ACK timeout".format(vm, MEMBERSHIP[vm][0]))
    return 0



########## Basic Functions for MP3 ###########
## Emulate crashing process
def sigint_handler(sig, frame):
    # Delete all files
    os.system("rm -rf " + SDFS_DIR)
    # exit
    os._exit(-1)

# Process a single line of command
# Return code: 0 on success, -1 on error, -2 on abort/exit
def proc_cmd(cmd):
    # Membership commands
    if cmd == 'p':
        print('')
        print_all()
        print('')
        return 0
    elif cmd == 'l':
        leave()
        os.system("rm -rf " + SDFS_DIR)
        os._exit(-2)
        # never reach here
        return -2
    elif cmd == 'c':
        print('')
        print('{}:{}'.format(VM, TIMESTAMP))
        print('')
        return 0

    # File system commands
    args = cmd.split(' ')

    if args[0] == 'put' and len(args) == 3:
        return put_file(args[1], args[2])

    elif args[0] == 'get' and len(args) == 3:
        return get_file(args[1], args[2])

    elif args[0] == 'delete' and len(args) == 2:
        return delete_file(args[1])

    elif args[0] == 'ls' and len(args) == 2:
        return ls_all_addresses(args[1])

    elif args[0] == 'store' and len(args) == 1:
        return ls_all_files()

    elif args[0] == 'get-versions' and len(args) == 4:
        return get_versions(args[1], args[2], args[3])

    # No commands found
    print("Command not found!")
    return -1

# Convert a string to an int in 1-9
def get_key(filename):
    return abs(hash(filename)) % 9 + 1

# Replicate the given sdfs_name file to other nodes
def replicate(sdfs_name, vm_list):
    # find which vms to send to
    send_to = []
    MBSLOCK.acquire()
    new_vm_list = get_vms(get_key(sdfs_name), W)
    MBSLOCK.release()
    for vm in new_vm_list:
        if vm not in vm_list:
            send_to.append(vm)

    # send to new replica nodes
    for vm in send_to:
        send_file('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), None, sdfs_name, saveOption=1, vm_list=new_vm_list, version=FILE_VER[sdfs_name])

    # update own file list
    FILELOCK.acquire()
    NAMESPACE[sdfs_name] = list(new_vm_list)
    for v in send_to:
        INV_NAMES[v].append(sdfs_name)
    FILELOCK.release()

    # send to old members
    cmdmsg = b'7|0|'+sdfs_name.encode('utf-8')+b'|'+pickle.dumps(new_vm_list)
    for vm in vm_list:
        if vm != VM:
            send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), FCMD_PORT, cmdmsg)
    return 0

# Send a file to the desired vm
## If local_name is None, send sdfs file instead
## saveOption: 0-> save from sdfs to local ; 1 -> save from local to sdfs
def send_file(addr, local_name, sdfs_name, saveOption=0, vm_list=[0,0,0,0], version=0):
    # try:
    read_file = local_name
    if local_name == None or saveOption == 0:
        read_file = SDFS_DIR+sdfs_name+str(FILE_VER[sdfs_name])
    print("Send file {} to {}".format(read_file, addr))
    logging.info("{}:{} Send file {} to {}".format(VM,TIMESTAMP,read_file, addr))
    if not os.path.isfile(read_file):
        return 0

    saveOp = '{}'.format(saveOption)

    ## version number and length of sdfs_name are sent as 5-digit strings
    if local_name == None:
        ## Send sdfs file between process
        new_v = '{:05}'.format(version)
        local_name = ""
    elif saveOption == 1:
        ## Save local file into sdfs
        new_v = '{:05}'.format(version + 1)
    else:
        ## Save sdfs file to local
        new_v = '{:05}'.format(FILE_VER[sdfs_name])


    local_len = '{:05}'.format(len(local_name))
    sdfs_len = '{:05}'.format(len(sdfs_name))
    dest = socket.create_connection((addr, FDATA_PORT))
    dest.send(saveOp)
    dest.send(new_v)
    dest.send(local_len)
    dest.send(local_name)
    dest.send(sdfs_len)
    dest.send(sdfs_name)
    for v in vm_list:
        dest.send(str(v))

    try:
        with open(read_file, 'rb') as locf:
            data = locf.read(1024)
            while data:
                dest.send(data)
                data = locf.read(1024)
    except:
        print(".")
    return 1

# Get number of vms
# Return a list of vm numberes
def get_vms(key, num_vms):
    vms = []
    i = key
    while(True):
        if MEMBERSHIP[i][2] == 1:
            vms.append(i)
        i = i%9 + 1
        if len(vms) >= num_vms:
            break
    return vms

# Return the highset version of current file
def get_version(sdfs_name):
    key = get_key(sdfs_name)
    # Just ping the first alive process with replica to get the version number
    for vm in get_vms(key,R):
        send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), FCMD_PORT, b'6|'+pickle.dumps({"sdfs_name":sdfs_name}))
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('', OTHER_PORT))
    msg, addr = s.recvfrom(5)
    s.close()
    version = int(msg)
    return version

## Check if a sdfs exist
def sdfs_file_exist(vm, sdfs_name, version):
    send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), FCMD_PORT, b'8|'+pickle.dumps({"sdfs_name":sdfs_name,"version":version}))
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('', OTHER_PORT))
    s.settimeout(1)
    try:
        msg, addr = s.recvfrom(1)
    except socket.timeout:
        return False
    s.close()
    if msg == "1":
        return True
    else:
        return False


# Put file onto SDFS
def put_file(local_name, sdfs_name):
    #print("Put file {} as {}".format(local_name, sdfs_name))
    logging.info("{}:{} Put file {} as {}".format(VM,TIMESTAMP,local_name, sdfs_name))
    # check existance
    #start = time.time()
    if not os.path.isfile(local_name):
        print("{} not found".format(local_name))
        return -1
    key = get_key(sdfs_name)
    vms_list = get_vms(key, W)
    v = get_version(sdfs_name)
    for vm in vms_list:
    #for vm in get_vms(key, 1):
        send_file('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), local_name, sdfs_name, saveOption=1, vm_list=vms_list, version=v)
    #done = time.time()
    #elapsed = done - start
    #print("PUT:{}".format(elapsed))
    return 0

# Get file from SDFS
def get_file(sdfs_name, local_name):
    key = get_key(sdfs_name)
    # Get vms with potential file
    logging.info("{}:{} Get file {} as {}".format(VM,TIMESTAMP,sdfs_name, local_name))
    file_exist = False
    for vm in get_vms(key, R):
        if sdfs_file_exist(vm, sdfs_name, -1):
            file_exist = True
            send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), FCMD_PORT, b'3|'+pickle.dumps({"sdfs_name":sdfs_name,"local_name":local_name}))
    if not file_exist:
        print("{} not found".format(sdfs_name))
    return 0

# Delete file fron SDFS
def delete_file(sdfs_name):
    logging.info("{}:{} Delete file {}".format(VM,TIMESTAMP,sdfs_name))
    for vm in range(1,10):
        send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), FCMD_PORT, b'5|'+pickle.dumps({"sdfs_name":sdfs_name}))
    return 0


def ls_all_addresses(sdfs_name):
    key = get_key(sdfs_name)
    vm_list = get_vms(key,W)
    logging.info("{}:{} List all addresses of file {}".format(VM,TIMESTAMP,sdfs_name))
    if not sdfs_file_exist(vm_list[0],sdfs_name, -1):
        print("{} not found".format(sdfs_name))
    else:
        for vm in vm_list:
            print("{}".format(vm))
    return 0

# Store command
def ls_all_files():
    logging.info("{}:{} List all of sdfs_file".format(VM,TIMESTAMP))
    for sdfs_file in FILE_VER:
        print("{}:v{}".format(sdfs_file, FILE_VER[sdfs_file]))
    return 0

def get_versions(sdfs_name, num_vs, local_name):
    logging.info("{}:{} Get latest {} versions of sdfs_file {} to local_file {}".format(VM,TIMESTAMP, num_vs ,sdfs_name,local_name))
    #start = time.time()
    num_vs = int(num_vs)
    key = get_key(sdfs_name)
    vm_list = get_vms(key,R)
    latest_version = get_version(sdfs_name)
    num_vs_needed = min(num_vs, latest_version)
    addr = 'fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm_list[0])
    s = socket.create_connection((addr, OTHER_PORT))
    s.send(pickle.dumps({"sdfs_name":sdfs_name,"num_vs":num_vs_needed}))

    with open(local_name,"w") as f:
        while True:
            data = s.recv(1024)
            if not data:
                success = True
                break
            f.write(data)
    s.close()
    #done = time.time()
    #elapsed = done - start
    #print("Get Versions:{}".format(elapsed))
    return 0




########## Threads to initialize ###########
###### File system threads here ######
def versions_sender():
    #print("File receiver initialized")
    ## Initialize TCP socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## Allow computer from other network to access the port
    s.bind(('', OTHER_PORT))
    s.listen(10)
    while not LEAVE:
        conn, addr = s.accept()
        msg = conn.recv(512)
        rcv_dict = pickle.loads(msg)
        sdfs_name = rcv_dict["sdfs_name"]
        num_vs = rcv_dict["num_vs"]
        cur_version = FILE_VER[sdfs_name]

        while num_vs != 0:
            read_file = SDFS_DIR+sdfs_name+str(cur_version)
            conn.send("v{}\n=========================\n".format(cur_version))
            with open(read_file, 'rb') as f:
                data = f.read(1024)
                while data:
                    conn.send(data)
                    data = f.read(1024)
            num_vs -= 1
            cur_version -=1
        conn.close()
    s.close()


def replicate_check():
    # Get old membership list
    MBSLOCK.acquire()
    mbs_new = MEMBERSHIP.copy()
    MBSLOCK.release()
    while not LEAVE:
        mbs_old = mbs_new.copy()
        # Wait for 1 second
        time.sleep(1)
        # start = time.time()
        # Re-acquire membership list
        MBSLOCK.acquire()
        mbs_new = MEMBERSHIP.copy()
        MBSLOCK.release()
        # Check changes
        names_change = {}
        for i in range(1, 10):
            if (mbs_new[i][1] == mbs_old[i][1] and mbs_new[i][2] == 2 and mbs_old[i][2] == 1) or (mbs_new[i][1] == mbs_old[i][1]+1 and mbs_new[i][2] == 0 and mbs_old[i][2] == 1):
                FILELOCK.acquire()
                try:
                    for files in INV_NAMES[i]:
                        NAMESPACE[files].remove(i)
                        names_change[files] = list(NAMESPACE[files])
                    INV_NAMES[i] = []
                except Exception as e:
                    print("Exception")
                    print(e)
                FILELOCK.release()

        # send files
        for files in names_change:
            if VM == min(names_change[files]):
                # Need replicate
                replicate(files, names_change[files])
        # done =time.time()
        # elapsed = done - start
        # if VM == 1:
        #     print(elapsed)


def file_receiver():
    #print("File receiver initialized")
    ## Initialize TCP socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## Allow computer from other network to access the port
    s.bind(('', FDATA_PORT))
    s.listen(10)
    while not LEAVE:
        conn, addr = s.accept()

        saveOption = int(conn.recv(1))
        version = int(conn.recv(5))
        local_len = int(conn.recv(5))
        local_name = str(conn.recv(local_len))
        sdfs_len = int(conn.recv(5))
        sdfs_name = str(conn.recv(sdfs_len))
        vm_list = [0, 0, 0, 0]
        vm_list[0] = int(conn.recv(1))
        vm_list[1] = int(conn.recv(1))
        vm_list[2] = int(conn.recv(1))
        vm_list[3] = int(conn.recv(1))

        print("{}(v{}) received".format(sdfs_name, version))
        logging.info("{}:{} {}(v{}) received".format(VM,TIMESTAMP,sdfs_name,version))
        # start = time.time()
        # Save to sdfs
        if saveOption == 1:
            save_file = SDFS_DIR + sdfs_name + str(version)


            last_ver = -1

            ## if this file already exist
            if sdfs_name in FILE_VER:
                last_ver = FILE_VER[sdfs_name]
                if FILE_VER[sdfs_name] >= version:
                    conn.close()
                    return -1
            FILELOCK.acquire()
            FILE_VER[sdfs_name] = version
            FILELOCK.release()
        # Save to local
        else:
            save_file = local_name


        print("Write to {}".format(save_file))
        logging.info("{}:{} {}(v{}) write to {}".format(VM,TIMESTAMP,sdfs_name,version,save_file))
        # Create new file
        success = False
        with open(save_file,"w+") as f:
            while not LEAVE:
                ## Receive the file
                try:
                    data = conn.recv(1024)
                    if not data:
                        success = True
                        break
                    f.write(data)

                except socket.error:
                    print("Error Occured.")
                    if saveOption == 1:
                        # Delete temporary file
                        os.system("rm -f " + save_file)
                        # Recover to old version
                        FILELOCK.acquire()
                        if last_ver == -1:
                            FILE_VER.pop(sdfs_name, None)
                        else:
                            FILE_VER[sdfs_name] = last_ver
                        FILELOCK.release()
                    break
        if success and vm_list[0] != 0:
            FILELOCK.acquire()
            if not sdfs_name in NAMESPACE:
                NAMESPACE[sdfs_name] = list(vm_list)
            elif sorted(NAMESPACE[sdfs_name]) != sorted(vm_list):
                NAMESPACE[sdfs_name] = list(vm_list)
            for v in vm_list:
                if not sdfs_name in INV_NAMES[v]:
                    INV_NAMES[v].append(sdfs_name)
            FILELOCK.release()
        # done = time.time()
        # elapsed = done - start
        # if saveOption == 0:
        #     print("GET:{}".format(elapsed))
        conn.close()

def cmd_receiver():
    ## Initialize UDP socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ## Allow computer from other network to access the port
    s.bind(('', FCMD_PORT))
    while not LEAVE:
        ## Receive and decode message if necessary
        msg, addr = s.recvfrom(MAX_CMD_LEN)
        msg_dec = msg.split(b'|')
        cmd = int(msg_dec[0])


        if cmd == 0:
            # CMD: ASK
            rcv_dict = pickle.loads(msg_dec[1])
            # Construct dict for request
            req_list = []
            for fname in rcv_dict:
                # check necessity
                FILELOCK.acquire()
                if fname not in FILE_VER or FILE_VER[fname] < rcv_dict[fname]:
                    # add to request
                    req_list.append(fname)
                    # update cur_version to prevent collision
                    FILE_VER[fname] = rcv_dict[fname]
                FILELOCK.release()
            # Send for request
            send_mesg(addr[0], FCMD_PORT, b'1|' + pickle.dumps(req_list))
            logging.info("{}:{} send request to {}".format(VM,TIMESTAMP, addr[0]))
        elif cmd == 1:
            rcv_list = pickle.loads(msg_dec[1])
            # CMD: REQUEST
            for f in rcv_list:
                send_file(addr[0], None, f, saveOption=1, version=FILE_VER[f])
            logging.info("{}:{} send files to {}".format(VM,TIMESTAMP, addr[0]))
        elif cmd == 2:
            # CMD: PRELEAVE --- obsolete!
            # send to another successor
            # send_file('''next successor''', None, file_name)
            return
        elif cmd == 3:
            # CMD: GET SDFILE TO LOCAL
            rcv_dict = pickle.loads(msg_dec[1])
            sdfs_name = rcv_dict["sdfs_name"]
            local_name = rcv_dict["local_name"]
            send_file(addr[0], local_name, sdfs_name)

        elif cmd == 4:
            # CMD: NEWJOIN
            newvm = int(msg_dec[1])
            for f in FILE_VER:
                FILELOCK.acquire()
                old_vmlist = list(NAMESPACE[f])
                FILELOCK.release()
                new_vmlist = old_vmlist + [newvm]

                # Decide which to remove
                deleted_vm = 0
                if len(new_vmlist) == 5:
                    distance = []
                    for i in range(len(new_vmlist)):
                        temp = new_vmlist[i] - get_key(f)
                        if temp < 0:
                            temp += 9
                        distance.append(temp)
                    deleted_vm = new_vmlist.pop(distance.index(max(distance)))

                # Send new VM list
                send_file('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(newvm), None, f, saveOption=1, vm_list=new_vmlist, version=FILE_VER[f])

                # update own file list
                if VM == deleted_vm:
                    # delete own
                    FILELOCK.acquire()
                    for v in NAMESPACE[f]:
                        INV_NAMES[v].remove(f)
                    NAMESPACE.pop(f, None)
                    FILELOCK.release()
                else:
                    if deleted_vm != 0:
                        # delete old
                        FILELOCK.acquire()
                        NAMESPACE[f].remove(deleted_vm)
                        INV_NAMES[deleted_vm].remove(f)
                        FILELOCK.release()
                    # insert new
                    FILELOCK.acquire()
                    NAMESPACE[f].append(newvm)
                    INV_NAMES[newvm].append(f)
                    FILELOCK.release()

                # send to other members
                cmdmsg = b'7|'+str(deleted_vm).encode('utf-8')+b'|'+f.encode('utf-8')+b'|'+pickle.dumps(new_vmlist)
                for vm in old_vmlist:
                    if vm != VM and vm != newvm:
                        send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(vm), FCMD_PORT, cmdmsg)
            logging.info("{}:{} send all files to fa18-cs425-g55-{:02d}.cs.illinois.edu".format(VM, TIMESTAMP, newvm))
        elif cmd == 5:
            # CMD: DELETE FILE
            rcv_dict = pickle.loads(msg_dec[1])
            sdfs_name = rcv_dict["sdfs_name"]
            # Just check all the version if exist. If exist delete it.
            if sdfs_name in FILE_VER:
                for i in range(1, FILE_VER[sdfs_name]+1):
                    if os.path.isfile(SDFS_DIR+sdfs_name+str(i)):
                        os.system("rm " + SDFS_DIR+sdfs_name+str(i))
                FILE_VER.pop(sdfs_name,None)
        elif cmd == 6:
            # CMD: Get version number
            rcv_dict = pickle.loads(msg_dec[1])
            sdfs_name = rcv_dict["sdfs_name"]
            if(sdfs_name not in FILE_VER):
                v = '{:05}'.format(0)
            else:
                v = '{:05}'.format(FILE_VER[sdfs_name])
            send_mesg(addr[0], OTHER_PORT, v)
        elif cmd == 7:
            # CMD: NAMESPACE update
            deleted_vm = int(msg_dec[1])
            sdfs_name = str(msg_dec[2].decode('utf-8'))
            vm_list = pickle.loads(msg_dec[3])
            if not VM in vm_list:
                # delete this file, then return
                FILELOCK.acquire()
                for v in NAMESPACE[sdfs_name]:
                    INV_NAMES[v].remove(sdfs_name)
                NAMESPACE.pop(sdfs_name)
                FILELOCK.release()
                return
            if deleted_vm != 0:
                # from_vm delete itself from list, also delete it
                FILELOCK.acquire()
                NAMESPACE[sdfs_name].remove(deleted_vm)
                INV_NAMES[deleted_vm].remove(sdfs_name)
                FILELOCK.release()
            FILELOCK.acquire()
            for v in vm_list:
                if not v in NAMESPACE[sdfs_name]:
                    NAMESPACE[sdfs_name].append(v)
                    INV_NAMES[v].append(sdfs_name)
            FILELOCK.release()
        elif cmd == 8:
            # CMD: Check if file with specific version exist,
            # if version is -1, just check if any version exist
            rcv_dict = pickle.loads(msg_dec[1])
            sdfs_name = rcv_dict["sdfs_name"]
            version = rcv_dict["version"]
            if version == -1:
                ret = sdfs_name in FILE_VER
            else:
                ret = os.path.isfile(SDFS_DIR+sdfs_name+str(version))
            if ret:
                send_mesg(addr[0], OTHER_PORT, "1")
            else:
                send_mesg(addr[0], OTHER_PORT, "0")


###### Membership threads below #######
def initialize_receiver():
    ## Initialize socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ## Allow computer from other network to access the port
    s.bind(('', RPORT))
    while not LEAVE:
        ## Receive and decode message if necessary
        msg, addr = s.recvfrom(VM_NUM * (TIMESTAMP_SIZE + INCARNATION_SIZE + STATUS_SIZE + 3) + VM_NUM_SIZE + 1)
        if msg == PING:
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
        plist = ping_to_list(VM, mbslist, 10)
        for v in plist:
            threading.Thread(target=ping_once, args=(v,)).start()

        ## Repeat pinging every INTERVAL seconds
        time.sleep(INTERVAL - ((time.time() - starttime) % INTERVAL))

# Incurred only for VM # 1
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
        # Tell the next node to share files
        send_mesg('fa18-cs425-g55-{:02d}.cs.illinois.edu'.format(get_vms(nn%9+1,1)[0]), FCMD_PORT, b'4|%s' % str(nn).encode('utf-8'))


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

    ## Initialize inverse Namespace
    for i in range(1, 10):
        INV_NAMES[i] = []

    ## Initialize timeout lists
    TOLIST = [False] * 10

    ## Make SDFS directory
    if not os.path.isdir("./"+SDFS_DIR):
        os.system("mkdir " + SDFS_DIR)

    ## Catch signals
    signal.signal(signal.SIGINT, sigint_handler)

    ## Start
    rec_tr = threading.Thread(target=initialize_receiver)
    rec_tr.start()
    if VM == ITDN:
        ind_tr = threading.Thread(target=initialize_introducer)
        ind_tr.start()
    else:
        rep_che_tr = threading.Thread(target=replicate_check)
        rep_che_tr.start()
        ver_sen_tr = threading.Thread(target=versions_sender)
        ver_sen_tr.start()
        file_rec_tr = threading.Thread(target=file_receiver)
        file_rec_tr.start()
        cmd_rec_tr = threading.Thread(target=cmd_receiver)
        cmd_rec_tr.start()

    pin_tr = threading.Thread(target=initialize_pinger)
    pin_tr.start()








    while(1):
        print('==========')
        print('Type \'c\' to print out self-id')
        print('Type \'p\' to print out the membership list')
        print('Type \'l\' to leave')
        print('Or type other commands')
        print('')
        ret_val = proc_cmd(raw_input())
        if ret_val == -2:
            print('Exiting...')
            break

    rec_tr.join()
    ind_tr.join()
    if ind_tr != None:
        ind_tr.join()
    pin_tr.join()
    file_rec_tr.join()
    cmd_rec_tr.join()
    ## Delete SDFS directory
    os.system("rm -rf " + SDFS_DIR)
