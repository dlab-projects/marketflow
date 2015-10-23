from os import listdir
import taq


def test_data_available():
    '''Test that our sample data is present

    Currently, data should be exactly the data also available on Box in the
    taq-data folder maintained by D-Lab. These data are copyrighted, so if
    you're not a member of the D-Lab, you'll likely need to arrange your own
    access!
    '''
    test_data_contents = listdir('test-data')
    assert 'EQY_US_ALL_BBO_20111101.zip' in test_data_contents
    assert 'EQY_US_ALL_BBO_20140206.zip' in test_data_contents
