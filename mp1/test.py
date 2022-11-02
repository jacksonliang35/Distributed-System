import unittest
from new_client import grep_from_mesg

class GrepTestCase(unittest.TestCase):

	def test_invalid_connection(self):
		ret = grep_from_mesg([11,'INFO','test'])
		self.assertEqual(ret, -1)

	def test_invalid_command(self):
		ret = grep_from_mesg([1,'-v d d','test'])
		self.assertEqual(ret, -2)

	
	def test_no_match(self):
		ret = grep_from_mesg([1,'THISFORTEST','test'])
		self.assertEqual(ret, 0)

	def test_rare_pattern(self):
		ret1 = grep_from_mesg([1, 'ERROR','test'])
		ret2 = grep_from_mesg([2, 'ERROR','test'])
		ret3 = grep_from_mesg([3, 'ERROR','test'])
		ret_total = ret1 + ret2 + ret3
		self.assertGreaterEqual(6,ret_total)

	def test_infrequent_pattern(self):
		ret1 = grep_from_mesg([1, 'WARNING','test'])
		ret2 = grep_from_mesg([2, 'WARNING','test'])
		ret3 = grep_from_mesg([3, 'WARNING','test'])
		ret_total = ret1 + ret2 + ret3
		self.assertGreaterEqual(1500*1/2 + 1500*2/2 + 1500*3/2,ret_total)

	def test_frequent_pattern(self):
		ret1 = grep_from_mesg([1, 'INFO','test'])
		ret2 = grep_from_mesg([2, 'INFO','test'])
		ret3 = grep_from_mesg([3, 'INFO','test'])
		ret_total = ret1 + ret2 + ret3
		self.assertGreaterEqual(ret_total,1500*1/2 + 1500*2/2 + 1500*3/2)
	
	def test_pattern_on_all_machine(self):
		ret1 = grep_from_mesg([1, 'root','test'])
		ret2 = grep_from_mesg([2, 'root','test'])
		ret3 = grep_from_mesg([3, 'root','test'])
		count = sum([i > 0 for i in [ret1, ret2, ret3]])
		self.assertEqual(count, 3)
	
	def test_pattern_on_some_machine(self):
		ret1 = grep_from_mesg([1, 'root','user1'])
		ret2 = grep_from_mesg([2, 'root','user1'])
		ret3 = grep_from_mesg([3, 'root','user1'])
		count = sum([i > 0 for i in [ret1, ret2, ret3]])
		self.assertGreater(3, count)
		
		

if __name__ == '__main__':
	unittest.main()

