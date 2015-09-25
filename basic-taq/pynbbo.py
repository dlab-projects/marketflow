#!/usr/bin/env python2

'''pynbbo.py - compute the NBBO from TAQ quote files

We're using python 2, because that's what's available on the clusters
'''

from datetime import datetime
import numpy as np
import tables as tbl
import sas7bdat as sas

# select column numbers
PERMNO = 0
TIME_M = 4
EX = 5
BID = 7
sym_root = 2
time_mtrail = 3
time_mroot = 11


class ExchangeBids(tbl.IsDescription):
    '''Table structure for bids / exchange'''

    # HDF5 support for time is funny / annoying - so we don't use it
    time_mroot = tbl.Int32Col()
    time_mtrail = tbl.Int32Col()
    A = tbl.Float32Col(1, dflt=np.nan)
    B = tbl.Float32Col(1, dflt=np.nan)
    C = tbl.Float32Col(1, dflt=np.nan)
    D = tbl.Float32Col(1, dflt=np.nan)
    I = tbl.Float32Col(1, dflt=np.nan)
    J = tbl.Float32Col(1, dflt=np.nan)
    K = tbl.Float32Col(1, dflt=np.nan)
    M = tbl.Float32Col(1, dflt=np.nan)
    N = tbl.Float32Col(1, dflt=np.nan)
    T = tbl.Float32Col(1, dflt=np.nan)
    P = tbl.Float32Col(1, dflt=np.nan)
    S = tbl.Float32Col(1, dflt=np.nan)
    Q = tbl.Float32Col(1, dflt=np.nan)
    W = tbl.Float32Col(1, dflt=np.nan)
    X = tbl.Float32Col(1, dflt=np.nan)
    Y = tbl.Float32Col(1, dflt=np.nan)
    Z = tbl.Float32Col(1, dflt=np.nan)


class BBids:
    '''Provide iterators over milliseconds for securities'''

    def __init__(self, fname):
        self.infile = sas.SAS7BDAT(fname)
        self.lines = self.infile.readlines()
        # For now, we just throw this out
        header = self.lines.next() # noqa

    def permno_row(self, permno):
        permno = int(permno)
        permno_table = self.h5f.create_table('/', 'p{}'.format(permno),
                                             ExchangeBids,
                                             "Best bids for {}".format(permno))
        return permno_table.row

    def record_time(self, row, time):
        row['time_mroot'] = (time.hour * 24 * 60 +
                             time.minute * 60 +
                             time.second)
        row['time_mtrail'] = time.microsecond / 1000

    def best_per_exchange(self, h5name):  #noqa
        '''compute a row for each present millisecond with max trades'''
        self.h5f = tbl.open_file(h5name, mode="w",
                                 title="Best bid per exchange")

        curr_time = None
        curr_permno = None

        for row in self.lines:
            permno = row[PERMNO]
            if curr_permno is None:
                # Our first security
                curr_permno = permno
                # This now works nicely with Slurm
                print permno, row[sym_root]
                out_row = self.permno_row(permno)

                # We keep track of how many stocks we've seen so we can quit
                # early
                num_seen = 1
            elif curr_permno != permno:
                # We have a new security
                out_row.append()
                num_seen += 1

                if permno == 0.0 or num_seen > 25:
                    # sas7bdat doesn't raise a StopIteration exception
                    # it just starts returning empty rows
                    break
                curr_permno = permno
                print permno, row[sym_root]
                curr_time = None
                out_row = self.permno_row(permno)

            if curr_time is None:
                # First time for this security and day
                curr_time = row[TIME_M]
                self.record_time(out_row, curr_time)
            elif curr_time != row[TIME_M]:
                # We are now in a new millisecond
                curr_time = row[TIME_M]
                out_row.append()
                self.record_time(out_row, curr_time)

            # Keep track of max bid per exchange from this millisecond
            # Default is np.nan in our HDF5 row
            # Some rows from Justin's file are 0.0, which should probably be
            # treated specially
            ex = row[EX]
            if out_row[ex] == np.nan:
                out_row[ex] = row[BID]
            elif out_row[ex] < row[BID]:
                out_row[ex] = row[BID]

        self.h5f.close()

if __name__ == '__main__':
    from sys import argv

    tick = datetime.now()

    sourcefiles_path = '/global/scratch/jmccrary/jmccrary/cqm_{}.sas7bdat'
    bbids = BBids(sourcefiles_path.format(argv[1]))

    output_path = '/global/scratch/davclark/benchmarking_data/nbbo_{}.h5'
    bbids.best_per_exchange(output_path.format(argv[1]))

    tock = datetime.now()

    # with open('{}_elapsed.txt'.format(argv[1]), 'w') as f:
    #     f.write('Elapsed time: {}\n'.format(tock - tick))

    print 'Elapsed time: {}\n'.format(tock - tick)
