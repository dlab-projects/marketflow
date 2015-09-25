import re
import datetime

def readfile(filename):
	f = open(filename, "r")
	mylog = []
	for row in f:
		mylog.append(row)
	searchstart = re.search(r'\d\d:\d\d:\d\d', mylog[0])
	searchend = re.search(r'\d\d:\d\d:\d\d', mylog[len(mylog)-1])
	start = datetime.datetime.strptime(searchstart.group(), '%H:%M:%S')
	end = datetime.datetime.strptime(searchend.group(), '%H:%M:%S')
	print(end - start)
	f.close()

from sys import argv
file = 'taq_htc.' + argv[1] + '.log'
#file = argv[1] + '.txt'
readfile(file)