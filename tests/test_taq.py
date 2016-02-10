from os import listdir

import pytest
from pytest import mark
import raw_taq as taq
import configparser
from os import path

test_path = path.dirname(__file__)
sample_data_dir = path.join(test_path, '../test-data/')
config = configparser.ConfigParser()
config.read(path.join(test_path, 'test_taq.ini'))
DATA_FILES = [y for x, y in config.items('taq-data')]


# sample = taq.TAQ2Chunks(sample_data_dir+DATA_FILES[0])
# chunk = next(sample.iter_)
# print (len(chunk.dtype))
# print(len(chunk[0]))

# print (chunk.dtype)
# print (chunk)
# print (type(chunk.dtype.names))

# print (type(chunk[0][4]))

# We can set up some processing this way
# Docs here: http://pytest.org/latest/fixture.html
@pytest.fixture(scope='module')
def h5_files():
    # XXX Update to be appropriate conversion to HDF5
    for i in range(len(DATA_FILES)):

        test_file = DATA_FILES[i]
        # Generate name for output file. Assumes filename of form
        # "EQY_US_ALL_BBO_YYYYMMDD.zip"
        out_name = test_file[15:23]

        # type(sample) is raw_taq.TAQ2Chunks
        sample = taq.TAQ2Chunks(test_file)

        # XXX use temp files / directories to store data
        # http://pytest.org/latest/tmpdir.html
        print ("+++ Creating log file for [" + test_file +
               "] as ./test-logs/"+out_name+"_log.txt")
        with open("test-logs/"+out_name+"_log.txt", 'w') as log:
            for chunk in sample.iter_:
                # chunk is a numpy array of tuples
                # print (type(chunk[0]))

                sorted_dtype = [(x,str(y[0])) for
                                x,y in sorted(chunk.dtype.fields.items(),
                                              key=lambda k: k[0])]

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
def test_row_values(fname):
    sample = taq.TAQ2Chunks(sample_data_dir+fname)
    chunk = next(sample.iter_)
    assert len(chunk) == sample.chunksize

    
    
    first_row_vals = {}

    for (x,y) in config.items('file1-row-values'):
        first_row_vals[x] = y

    print (first_row_vals)

    field_mapping = {}
    field_names = chunk.dtype.names
    i = 0
    for field in field_names:
        field_lower = field.lower()
        field_mapping[field_lower] = str(chunk[0][i])
        i += 1
        assert field_mapping[field_lower] == first_row_vals[field_lower]
    print (field_mapping)



    # assert sample.numlines =

    # $chunk$ is a numpy.ndarray that we can index into



@mark.parametrize('fname', DATA_FILES)
def test_statistics(fname):
    # np.average()
    print('hi')


@mark.xfail
def test_hdf5_rows_match_input(fname, h5_files):
    # XXX h5 files will return a list of files it's created
    raise NotImplementedError


if __name__ == '__main__':
    pytest.main("test_taq.py")
