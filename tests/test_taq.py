import taq
import arrow
import pytest
import numpy as np
import configparser
from os import path
from os import listdir
from pytest import mark
from zipfile import ZipFile
from dateutil.tz import gettz

# For comparison purposes
from pytz import timezone
import pytz


test_path = path.dirname(__file__)
sample_data_dir = path.join(test_path, '../test-data/')
config = configparser.ConfigParser()
config.read(path.join(test_path, 'test_taq.ini'))
DATA_FILES = [y for x, y in config.items('taq-data')]

# We can set up some processing this way
# Docs here: http://pytest.org/latest/fixture.html
 

@mark.xfail
@pytest.fixture(scope='module')
def h5_files(tmpdir):
    # XXX Update to be appropriate conversion to HDF5
    for i in range(len(DATA_FILES)):
        test_file = DATA_FILES[i]
        # Generate name for output file. Assumes filename of form
        # "EQY_US_ALL_BBO_YYYYMMDD.zip"
        out_name = test_file[15:23]
        sample = taq.TAQ2Chunks(test_file)

        # XXX use temp files / directories to store data
        # http://pytest.org/latest/tmpdir.html


        # empty hdf5 table?
        h5_table = sample.setup_hdf5('sample')

        h5_table.append(chunk)

        h5_table.close()

        return out_name  # or out_names ideally!


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
def test_row_values(fname, numlines=5):
    sample = taq.TAQ2Chunks(sample_data_dir+fname)
    chunk = next(sample)
    assert len(chunk) == sample.chunksize

    # Use raw_taq to read in raw bytes
    chunk_unprocessed_gen = taq.TAQ2Chunks(sample_data_dir+fname, chunksize=numlines, do_process_chunk=False)
    chunk_processed_gen = taq.TAQ2Chunks(sample_data_dir+fname, chunksize=numlines, do_process_chunk=True)
    chunk = next(chunk_unprocessed_gen)
    chunk_proc = next(chunk_processed_gen)

    month, day, year = chunk_unprocessed_gen.month, chunk_unprocessed_gen.day, chunk_unprocessed_gen.year

    for i in range(chunk.shape[0]):
        entry = chunk[i]
        msec = int(entry['msec'][2:5])

        date_object = arrow.Arrow(year, month, day, 
            hour=int(entry['hour']), 
            minute=int(entry['minute']), 
            second=int(entry['msec'][0:2]), 
            tzinfo=gettz('America/New York'))

        unix_time = date_object.timestamp + msec/1000

        # in bytes
        symbol_root, symbol_suffix = entry['Symbol_root'], entry['Symbol_suffix']
        bid_price = int(entry['Bid_Price'][0:7]) + int(entry['Bid_Price'][7:11])/10000
        bid_size = int(entry['Bid_Size'])
        ask_price = int(entry['Ask_Price'][0:7]) + int(entry['Ask_Price'][7:11])/10000
        ask_size = int(entry['Ask_Size'])

        print (bid_price, bid_size, ask_price, ask_size)


@mark.parametrize('fname', DATA_FILES)
def test_statistics(fname):
    # np.average()
    print('hi')


@mark.xfail
def test_hdf5_rows_match_input(fname, h5_files):
    # XXX h5 files will return a list of files it's created
    raise NotImplementedError


if __name__ == '__main__':
    # pytest.main("test_taq.py")

    test_row_values('EQY_US_ALL_BBO_20140206.zip')

