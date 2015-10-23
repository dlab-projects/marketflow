from os import listdir

from pytest import mark
import taq

# This should probably be moved to an ini file
DATA_FILES = ['EQY_US_ALL_BBO_20111101.zip',
              'EQY_US_ALL_BBO_20140206.zip']


@mark.parametrize('fname', DATA_FILES)
def test_data_available(fname):
    '''Test that our sample data is present

    Currently, data should be exactly the data also available on Box in the
    taq-data folder maintained by D-Lab. These data are copyrighted, so if
    you're not a member of the D-Lab, you'll likely need to arrange your own
    access!
    '''
    data_dir_contents = listdir('test-data')
    assert fname in data_dir_contents
