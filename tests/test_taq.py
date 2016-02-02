from os import listdir

from pytest import mark
import raw_taq as taq
import configparser
import numpy as np

sample_data_dir = '../test-data/'
config = configparser.ConfigParser()
config.read('./test_taq.ini')
DATA_FILES = [y for x, y in config.items('taq-data')]


# For sandbox purposes
# sample = taq.TAQ2Chunks(sample_data_dir+'EQY_US_ALL_BBO_20140206.zip')
# chunk = next(sample.iter_)
# print (chunk)
# print (type(chunk))
# print ("#######")
# print (chunk[0])
# print (chunk[999999])


@mark.parametrize('fname', DATA_FILES)
def test_data_available(fname):
    '''Test that our sample data is present
    Currently, data should be exactly the data also available on Box in the
    taq-data folder maintained by D-Lab. These data are copyrighted, so if
    you're not a member of the D-Lab, you'll likely need to arrange your own
    access!
    '''
    data_dir_contents = listdir(sample_data_dir)
    assert fname in data_dir_contents

@mark.parametrize('fname', DATA_FILES)
def test_row_values(fname):
    sample = taq.TAQ2Chunks(sample_data_dir+fname)
    chunk = next(sample.iter_)
    assert len(chunk) == 1000000

    # $chunk$ is a numpy.ndarray that we can index into
    





@mark.parametrize('fname', DATA_FILES)
def test_statistics(fname):
    # np.average()
    print('hi')

