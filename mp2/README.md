# CS425_MP2
Yuchen Liang (yliang35) and Xilun Jin (xjin12)
## Distributed Group Membership
### Files:

node.py

### Instruction:

* In each machine, run `python node.py current_vm_number`
* While running the process, you can type 'c' to print self id, 'p' to print membership list, and 'l' to leave
* Membership list

	```
		1:20181006013644 alive 1
	  vm_number+timestamp status status_change_number
	```

### Output:

app.log: log file

### Measure the bandwidth:
`sudo tcpdump -i eth0 -n -e -l port 12345 |perl netbps.pl`

note: netbps is from https://superuser.com/questions/356907/how-to-get-real-time-network-statistics-in-linux-with-kb-mb-bytes-format-and-for


