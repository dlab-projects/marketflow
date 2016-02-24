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
    chunk = next(sample.iter_)
    assert len(chunk) == sample.chunksize

    with ZipFile(sample_data_dir+fname) as zfile:
        for file in zfile.namelist():
            with zfile.open(file) as taqfile:

                # Call readline() once to read in the first row which has #lines
                record_count = taqfile.readline()
                line_length = len(record_count)

                # Read in raw bytes of lines 2-6 of file
                raw_bytes = taqfile.read(line_length * numlines)
                entries = [raw_bytes[i:i+line_length] for i in range(0,len(raw_bytes),line_length)]
                
                # Do a byte-field mapping using numpy
                dt = [  ('Hour',                       'S2'),
                        ('Minute',                     'S2'),
                        ('Second',                     'S2'),
                        ('Milliseconds',               'S3'),
                        ('Exchange',                   'S1'),
                        ('Symbol_Root',                'S6'),
                        ('Symbol_Suffix',             'S10'),
                        ('Bid_Price',                 'S11'),
                        ('Bid_Size',                   'S7'),
                        ('Ask_Price',                 'S11'),
                        ('Ask_Size',                   'S7'),
                        ('Quote_Condition',            'S1'),
                        ('Market_Maker',               'S4'),
                        ('Bid_Exchange',               'S1'),
                        ('Ask_Exchange',               'S1'),
                        ('Sequence_Number',           'S16'),
                        ('National_BBO_Ind',           'S1'),
                        ('NASDAQ_BBO_IND',             'S1'),
                        ('Quote_Cancel_Correction',    'S1'),
                        ('Source_of_Quote',            'S1'),
                        ('Retail_Interest_Ind',        'S1'),
                        ('Short_Sale_Restriction_Ind', 'S1'),
                        ('LULD_BBO_Ind_CQS',           'S1'),
                        ('LULD_BBO_Ind_UTP',           'S1'),
                        ('FINRA_ADF_MPID_Ind',         'S1'),
                        ('SIP_Generated_Message_ID',   'S1'),
                        ('National_BBO_LULD_Ind',      'S1'),
                        ('Line_Change',                'S2')  ]
                
                structured_byte_mapping = np.array(entries, dtype=dt)

                month, day, year = int(record_count[2:4]), int(record_count[4:6]), int(record_count[6:10])

                for i in range(len(structured_byte_mapping)):
                    entry = structured_byte_mapping[i]
                    date_object = arrow.Arrow(year, month, day, 
                        hour=int(entry['Hour']), 
                        minute=int(entry['Minute']), 
                        second=int(entry['Second']),
                        microsecond=1000*int(entry['Milliseconds']), 
                        tzinfo=gettz('America/New York'))

                    unix_time = date_object.timestamp + (int(entry['Milliseconds'])/1000)

                    # To confirm unix time results using online epoch converters, New York
                    # is GMT-5, so take whatever HHMMSS value and add 5 to HH

                    # Seems like vectorized arithmetic operations on numpy arrays rounds the thousandths place
                    # up to nearest hundredths place -- rounding error

                    assert chunk[i][0] == unix_time

                    # Should test other row values
                    symbol_root, symbol_suffix = entry['Symbol_Root'], entry['Symbol_Suffix']

                    bid_price, bid_size = entry['Bid_Price'], entry['Bid_Size']
                    ask_price, ask_size = entry['Ask_Price'], entry['Ask_Size']
                    assert len(bid_price) == 11
                    assert len(bid_size) == 7
                    assert len(ask_price) == 11
                    assert len(ask_size) == 7

                    # assert chunk[i][7] == bid_price
                    # assert chunk[i][8] == bid_size
                    # assert chunk[i][9] == ask_price
                    # assert chunk[i][10] == ask_size



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

