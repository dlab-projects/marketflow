
from os import listdir

import pytest

import time

from pytest import mark

import raw_taq as taq
import pandas as pd
import numpy as np
import sys
import os
import shutil


# Provide a list of all data files as a command line argv 
DATA_FILES = ['EQY_US_ALL_BBO_20140206.zip']


def read_command(argv):
    args = argv[1:]


@mark.parametrize('fname', DATA_FILES)
def test_data_available(fname):
    '''Test that our sample data is present

    Currently, data should be exactly the data also available on Box in the
    taq-data folder maintained by D-Lab. These data are copyrighted, so if
    you're not a member of the D-Lab, you'll likely need to arrange your own
    access!
    '''
    data_dir_contents = listdir('test-data')
    # assert fname in data_dir_contents
	

def test_row_values(fname):
	"""
	For each row in the TAQ file, 
	"""

	raise NotImplementedError

def test_hdf5_rows_match_input(fname):
	raise NotImplementedError


def clear_log_dir():
    print ("... Checking for existig files in test-logs/")
    if os.path.exists("./test-logs/"):
        out_exists_prompt = input("A log directory already exists. Would you like to overwrite it? (y/n) ")
        print()
        while out_exists_prompt not in ('y','yes','ye','yse','YES', 'Y', 'n', 'no', 'NO', 'N'):
            out_exists_prompt = input("Sorry, invalid response. Overwrite the log directory? (y/n) ")
        if out_exists_prompt in ('y','yes','ye','yse','YES', 'Y'):
            shutil.rmtree('./test-logs/')
            os.makedirs("./test-logs/")
            print ("+++ Created a new directory for test logs: test-logs/")
        elif out_exists_prompt in ('n', 'no', 'NO', 'N'):
            print ("Please save your output logs elsewhere and try again.")
            sys.exit()
    else:
        os.makedirs("./test-logs/")
        print ("+++ Created a new directory for test logs: test-logs/")


if __name__ == '__main__':
    options = read_command(sys.argv)

    # Prompt user to overwrite previous output files
    clear_log_dir()


    for i in range(len(DATA_FILES)):

        test_file = DATA_FILES[i]
        # Generate name for output file. Assumes filename of form "EQY_US_ALL_BBO_YYYYMMDD.zip"
        out_name = test_file[15:23]

        # type(sample) is raw_taq.TAQ2Chunks
        sample = taq.TAQ2Chunks(test_file)

        print ("+++ Creating log file for [" + test_file + "] as ./test-logs/"+out_name+"_log.txt")
        with open("test-logs/"+out_name+"_log.txt", 'w') as log:
            for chunk in sample.iter_:
                # chunk is a numpy array of tuples
                # print (type(chunk[0]))

                sorted_dtype = [(x,str(y[0])) for x,y in sorted(chunk.dtype.fields.items(),key=lambda k: k[0])]

                for attr, type in sorted_dtype:
                    log.write(attr + "     ")

                # for attr, type in chunk.dtype.fields.items():
                #     print (attr)
                #     print ("    ")

                # print (chunk[0][0])
                # print (chunk[0][1])

                # placeholder write to log txt file
                # log.write("data\n")


        # empty hdf5 table?
        h5_table = sample.setup_hdf5('sample')

        h5_table.append(chunk)


