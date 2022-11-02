# CS425_MP3

Yuchen Liang (yliang35) and Xilun Jin (xjin12)
## Distributed File System
### Files:

file_sys.py

### Instruction:

* In each machine, run `python files_sys.py current_vm_number`. Note that VM 10 should be run first (introducer).
* (from MP2) While running the process, you can type 'c' to print self id, 'p' to print membership list, and 'l' to leave
* (from MP2) Membership list

	```
		1:20181006013644 alive 1
	  vm_number+timestamp status status_change_number
	```
* You can also type in the required commands from MP3
	*	`put localfilename sdfsfilename` (from local dir)
	*	`get sdfsfilename localfilename` (fetch to local dir)
	*	`delete sdfsfilename`
	*	`store (at any process):` list the set of file names that are replicated (stored) on SDFS at that process. This should not include files stored on the local file system.
	*	`ls sdfsfilename:` list all VM addresses where this file is currently replicated (If you are splitting files into blocks, just set the block size to be large enough that each file is one block)
	*	`get-versions sdfsfilename num-versions localfilename` get all the last num-versions versions of the file into the localfilename(use delimiters to mark out versions).


### Output:

app.log: log file

### Measure the bandwidth:
`sudo tcpdump -i eth0 -n -e -l port 12545 |perl netbps.pl`
`sudo tcpdump -i eth0 -n -e -l port 12645 |perl netbps.pl`

note: netbps is from https://superuser.com/questions/356907/how-to-get-real-time-network-statistics-in-linux-with-kb-mb-bytes-format-and-for
