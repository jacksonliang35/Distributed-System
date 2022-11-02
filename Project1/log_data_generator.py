import sys
import random

# Randomly generate a type list
# Error is rare: [1,2]
# Info is frequent: [total_number/2 + 1, total_number -5]
# Warning is somewhat frequent: 1 - #Error - #Info
def generate_type_list(total_number):
	num_error = random.randint(1,2)
	num_info = random.randint(total_number/2+1, total_number-5)
	num_warning = total_number - num_error - num_info
	return ["ERROR"]*num_error + ["WARNING"]*num_warning + ["INFO"]*num_info


# Randomly generate a user list
# root is frequent: [1,total_number]
# user1 is only on odd-number vm
# user2 is only on even-number vm
def generate_user_list(total_number):
	num_root = random.randint(1,total_number)
	num_user1 = 0
	num_user2 = 0
	if total_number%2 == 1:
		num_user1 = total_number - num_root
	else:
		num_user2 = total_number - num_root
	return ["root"]*num_root + ["user1"]*num_user1 + ["user2"]*num_user2

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print("Please provide the vm number")
		sys.exit(-1)
	vm_number = int(sys.argv[1])

	#total number of logs is the 15 * the current vm number
	total_number = 1500 * vm_number

	#log file saved as test{vm_number}.log
	f = open("test{}.log".format(vm_number), "w+")

	messages = ["Hello", "Hi", "What's up"]
	type_list = generate_type_list(total_number)
	user_list = generate_user_list(total_number)

	while total_number > 0:
		type_selected = random.choice(type_list)
		type_list.remove(type_selected)
		user_selected = random.choice(user_list)
		user_list.remove(user_selected)
		message_selected = random.choice(messages)

		f.write("{} [2018-09-{:02d}] {} : {}\n".format(type_selected, vm_number, user_selected, message_selected))
		total_number -= 1
	f.close()
	
