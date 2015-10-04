#!/usr/bin/env python3

from os import path
from zipfile import ZipFile

from pytz import timezone
import numpy as np
from numpy.lib import recfunctions
import tables as tb
import time, datetime


class BytesSpec(object):
    '''A description of the records in raw TAQ files'''

    # List of (Name, # of bytes_spectes)
    # We will use this to contstuct "bytes" (which is what 'S' stands for - it
    # doesn't stand for "string")
    initial_dtype_info = [# Time is given in HHMMSSmmm, should be in Eastern Time (ET)
                          ('hour', 2),
                          ('minute', 2),
                          ('msec', 5), # This includes seconds - so up to
                                       # 59,999 msecs
                          ('Exchange', 1),
                          ('Symbol_root', 6),
                          ('Symbol_suffix', 10),
                          ('Bid_Price', 11),  # 7.4 (fixed point)
                          ('Bid_Size', 7),
                          ('Ask_Price', 11),  # 7.4
                          ('Ask_Size', 7),
                          ('Quote_Condition', 1),
                          # Market_Maker ends up getting discarded
                          # It should always be b'    '
                          ('Market_Maker', 4),
                          ('Bid_Exchange', 1),
                          ('Ask_Exchange', 1),
                          ('Sequence_Number', 16),
                          ('National_BBO_Ind', 1),
                          ('NASDAQ_BBO_Ind', 1),
                          ('Quote_Cancel_Correction', 1),
                          ('Source_of_Quote', 1),
                          ('Retail_Interest_Indicator_RPI', 1),
                          ('Short_Sale_Restriction_Indicator', 1),
                          ('LULD_BBO_Indicator_CQS', 1),
                          ('LULD_BBO_Indicator_UTP', 1),
                          ('FINRA_ADF_MPID_Indicator', 1),
                          ('SIP_generated_Message_Identifier', 1),
                          ('National_BBO_LULD_Indicator', 1),
                         ]

    # Justin and Pandas (I think) use time64, as does PyTables.
    # We could use msec from beginning of day for now in an uint16
    # (maybe compare performance to datetime64? Dates should compress very well...)

    convert_dtype = [# Time is the first field in HHMMSSmmm format
                     ('hour', np.int8),
                     ('minute', np.int8),
                     # This works well for now, but pytables wants:
                     # <seconds-from-epoch>.<fractional-seconds> as a float64
                     ('msec', np.uint16),
                     ('Bid_Price', np.float64),
                     ('Bid_Size', np.int32),
                     ('Ask_Price', np.float64),
                     ('Ask_Size', np.int32),
                     # This is not currently used, and should always be b'    '
                     # ('Market_Maker', np.int8),
                     ('Sequence_Number', np.int64),
                     # The _Ind fields are actually categorical - leaving as strings
                     # ('National_BBO_Ind', np.int8),
                     # ('NASDAQ_BBO_Ind', np.int8),
                    ]

    convert_dict = dict(convert_dtype)

    passthrough_strings = ['Exchange',
                           'Symbol_root',
                           'Symbol_suffix',
                           'Quote_Condition',
                           'Market_Maker',
                           'Bid_Exchange',
                           'Ask_Exchange',
                           'National_BBO_Ind',
                           'NASDAQ_BBO_Ind',
                           'Quote_Cancel_Correction',
                           'Source_of_Quote',
                           'Retail_Interest_Indicator_RPI',
                           'Short_Sale_Restriction_Indicator',
                           'LULD_BBO_Indicator_CQS',
                           'LULD_BBO_Indicator_UTP',
                           'FINRA_ADF_MPID_Indicator',
                           'SIP_generated_Message_Identifier',
                           'National_BBO_LULD_Indicator'
                          ]

    def __init__(self, bytes_per_line, computed_fields=None):
        '''Set up dtypes, etc. based on bytes_per_line

        bytes_per_line : int
            Should be one of two possible values XXX which are?
        computed_fields : [('Name', dtype), ...]
            A list-based structured dtype, for use for example with
            `[('Time', 'datetime64[ms]')]`.  PyTables will not accept
            np.datetime64, but we use it to work with the pytables_desc
            computed attribute.
        '''
        self.bytes_per_line = bytes_per_line
        self.check_present_fields()

        # The "easy" dtypes are the "not datetime" dtypes
        easy_dtype = []

        for name, dtype in self.initial_dtype:
            if name in self.convert_dict:
                easy_dtype.append( (name, self.convert_dict[name]) )
            elif name in self.passthrough_strings:
                easy_dtype.append( (name, dtype) )
            # Items not in these strings are silently ignored! We could add
            # logic to allow for explicitly ignoring fields here.

        if computed_fields:
            self.target_dtype = computed_fields + easy_dtype
        else:
            self.target_dtype = easy_dtype

    @property
    def pytables_desc(self):
        """
        Convert NumPy dtype to PyTable descriptor (lifted from blaze.pytables).
        Examples
        --------
        >>> from tables import Int32Col, StringCol, Time64Col
        >>> dt = np.dtype([('name', 'S7'), ('amount', 'i4'), ('time', 'M8[us]')])
        >>> dtype_to_pytables(dt)  # doctest: +SKIP
        {'amount': Int32Col(shape=(), dflt=0, pos=1),
         'name': StringCol(itemsize=7, shape=(), dflt='', pos=0),
         'time': Time64Col(shape=(), dflt=0.0, pos=2)}
        """
        dtype = np.dtype(self.target_dtype)

        d = {}
        for pos, name in enumerate(dtype.names):
            dt, _ = dtype.fields[name]
            if issubclass(dt.type, np.datetime64):
                tdtype = tb.defscription({name: tb.Time64Col(pos = pos)}),
            else:
                tdtype = tb.descr_from_dtype(np.dtype([(name, dt)]))
            el = tdtype[0]  # removed dependency on toolz -DJC
            getattr(el, name)._v_pos = pos
            d.update(el._v_colobjects)

        return d

    def check_present_fields(self):
        """
        self.initial_dtype_info should be of form, we encode newline info here!

        [('Time', 9),
         ('Exchange', 1),
         ...
        ]

        Assumption is that the last field is a newline field that is present in
        all versions of BBO
        """
        cum_len = 0
        self.initial_dtype = []

        # Newlines consume 2 bytes
        target_len = self.bytes_per_line - 2

        for field_name, field_len in self.initial_dtype_info:
            # Better to do nested unpacking within the function
            cum_len += field_len
            self.initial_dtype.append( (field_name, 'S{}'.format(field_len)) )
            if cum_len == target_len:
                self.initial_dtype.append(('newline', 'S2'))
                return

        raise Error("Can't map fields onto bytes_per_line")


class TAQ2Chunks:
    '''Read in raw TAQ BBO file, and return numpy chunks (cf. odo)'''

    # These are the data available in the header of the file
    numlines = None
    year = None
    month = None
    day = None

    # This is a totally random guess. It should probably be tuned if we care...
    DEFAULT_CHUNKSIZE = 1000000

    def __init__(self, taq_fname, chunksize=None, do_process_chunk=True):
        '''Configure conversion process and (for now) set up the iterator
        taq_fname : str
            Name of input file
        chunksize : int
            Number of rows in each chunk. If None, the HDF5 logic will set it
            based on the chunkshape determined by pytables. Otherwise,
            `chunks()` will set this to DEFAULT_CHUNKSIZE.
        do_process_chunk : bool
            Do type conversions?
        '''
        self.taq_fname = taq_fname
        self.chunksize = chunksize
        self.do_process_chunk = do_process_chunk

        self.iter_ = self._convert_taq()
        # Get first line read / set up remaining attributes
        next(self.iter_)

    def __len__(self):
        return self.numlines

    def __iter__(self):
        # Returning the internal iterator avoids a function call, not a big
        # deal, but may as well avoid extra computation
        return self.iter_

    def __next__(self):
        return next(self.iter_)

    def _convert_taq(self):
        '''Return a generator that yields chunks, based on config in object

        This is meant to be called from within `__init__()`, and stored in
        `self.iter_`
        '''
        # The below doesn't work for pandas (and neither does `unzip` from the
        # command line). Probably want to use something like `7z x -so
        # my_file.zip 2> /dev/null` if we use pandas.

        with ZipFile(self.taq_fname) as zfile:
            for inside_f in zfile.filelist:
                # The original filename is available as inside_f.filename
                self.infile_name = inside_f.filename

                with zfile.open(inside_f.filename) as infile:
                    first = infile.readline()
                    bytes_per_line = len(first)

                    if self.do_process_chunk:
                        self.bytes_spec = \
                            BytesSpec(bytes_per_line,
                                      computed_fields=[('Time', np.float64)])
                                      # We want this for making the PyTables
                                      # description:
                                      # computed_fields=[('Time', 'datetime64[ms]')])
                    else:
                        self.bytes_spec = BytesSpec(bytes_per_line)

                    # You need to use bytes to split bytes
                    # some files (probably older files do not have a record count)
                    try:
                        dateish, numlines = first.split(b":")
                        self.numlines = int(numlines)
                    except ValueError:
                        dateish = first

                    # Get dates to combine with times later
                    # This is a little over-trusting of the spec...
                    self.month = int(dateish[2:4])
                    self.day = int(dateish[4:6])
                    self.year = int(dateish[6:10])

                    # Nice idea from @rdhyee, we only need to compute the
                    # 0-second for the day once per file.self
                    naive_dt = datetime.datetime(self.year, self.month, self.day)

                    # It turns out you can't pass tzinfo directly, See
                    # http://pythonhosted.org/pytz/
                    # This lets us compute a UTC timestamp
                    self.midnight_ts = timezone('US/Eastern').\
                                        localize(naive_dt).\
                                         timestamp()

                    # This lets us parse the first line to initialize our
                    # various attributes
                    yield

                    if self.do_process_chunk:
                        for chunk in self.chunks(self.numlines, infile):
                            yield self.process_chunk(chunk)
                    else:
                        yield self.chunks(self.numlines, infile)

    def process_chunk(self, all_bytes):
        '''Convert the structured ndarray `all_bytes` to the target_dtype

        If you did not specify do_process_chunk, you might run this yourself on
        chunks that you get from iteration.'''
        # Note, this is slower than the code directly below
        # records = recfunctions.append_fields(easy_converted, 'Time',
        #                                      time64ish, usemask=False)
        target_dtype = np.dtype(self.bytes_spec.target_dtype)
        combined = np.empty(all_bytes.shape, dtype=target_dtype)

        # This should perform type coercion as well
        for name in target_dtype.names:
            if name == 'Time':
                continue
            combined[name] = all_bytes[name]

        # These don't have the decimal point in the TAQ file
        for dollar_col in ['Bid_Price', 'Ask_Price']:
            combined[dollar_col] /= 10000

        # Currently, there doesn't seem to be any value in converting to
        # numpy.datetime64, as PyTables wants float64's corresponding to the POSIX
        # Standard (relative to 1970-01-01, UTC) that it then converts to a
        # time64 struct on it's own

        # TODO This is the right math, but we still need to ensure we're
        # coercing to sufficient data types (we need to make some tests!).

        # The math is also probably a bit inefficient, but it seems to work,
        # and based on Dav's testing, this is taking negligible time compared
        # to the above conversions.
        time64ish = (self.midnight_ts +
                     combined['hour'] * 3600 +
                     combined['minute'] * 60 +
                     # I'm particularly amazed that this seems to work (in py3)
                     combined['msec'] / 1000)

        combined['Time'] = time64ish

        return combined

    def chunks(self, numlines, infile):
        '''Do the conversion of bytes to numpy "chunks"'''
        # TODO Should do check on numlines to make sure we get the right number

        if self.chunksize is None:
            self.chunksize = self.DEFAULT_CHUNKSIZE

        while(True):
            raw_bytes = infile.read(self.bytes_spec.bytes_per_line * self.chunksize)
            if not raw_bytes:
                break

            # This is a fix that @rdhyee made, but due to non-DRY appraoch, he
            # did not propagate his fix!
            all_bytes = np.ndarray(len(raw_bytes) // self.bytes_spec.bytes_per_line,
                                        buffer=raw_bytes,
                                        dtype=self.bytes_spec.initial_dtype)

            yield all_bytes

    # Everything from here down is HDF5 specific
    def setup_hdf5(self, h5_fname_root=None):
        '''Open an HDF5 file with pytables and return a reference to a table

        The table will be constructed based on self.bytes_spec.pytables_desc.

        h5_fname_root : str
            Used as `title` of HDF5 file, and '.h5' is appended to make the
            filename. If unspecified, this is derived from self.taq_fname by
            dorpping off the final extension.
        '''
        if h5_fname_root is None:
            h5_fname_root, _ = path.splitext(self.taq_fname)

        # We're using aggressive compression and checksums, since this will
        # likely stick around, I'm stopping one level short of max compression.
        # Don't be greedy :P
        self.h5 = tb.open_file(h5_fname_root + '.h5',
                               title=path.basename(h5_fname_root),
                               mode='w',
                               filters=tb.Filters(complevel=8,
                                                  complib='blosc:lz4hc',
                                                  fletcher32=True) )

        return self.h5.create_table('/', 'daily_quotes',
                                    description=self.bytes_spec.pytables_desc,
                                    expectedrows=self.numlines)

    def finalize_hdf5(self):
        self.h5.close()

    def to_hdf5(self):
        '''Read raw bytes from TAQ, write to HDF5'''
        # Should I use a context manager here?
        h5_table = self.setup_hdf5()

        # At some point, we might optimize chunksize. If we create our hdf5
        # file with PyTables before setting chunksize, we currently assume
        # PyTables is smart.
        if self.chunksize is None:
            self.chunksize = h5_table.chunkshape[0]

        try:
            for chunk in self.iter_:
                h5_table.append(chunk)
                # XXX for testing, we are only converting one chunk
                #break
        finally:
            self.finalize_hdf5()

if __name__ == '__main__':
    from sys import argv
    import os
    from tables import *

    class Particle(IsDescription):
        name = StringCol(30)   # 16-character String
        time = StringCol(8)

    fnames = argv[1:]   #./raw_taq.py ../../local_data/EQY_US_ALL_BBO_201502*.zip
    if not fnames:
        # Grab our agreed-upon "standard" BBO file
        fnames = ['../../local_data/EQY_US_ALL_BBO_20150202.zip']
        # fname = '../local_data/EQY_US_ALL_BBO_20140206.zip'


    def timing(t0, t1):
        #convert the time note to string with regular formate
        a = datetime.datetime.fromtimestamp(t0).strftime('%Y-%m-%d %H:%M:%S')
        b = datetime.datetime.fromtimestamp(t1).strftime('%Y-%m-%d %H:%M:%S')

        #time string calucation
        start = datetime.datetime.strptime(a, '%Y-%m-%d %H:%M:%S')
        end = datetime.datetime.strptime(b, '%Y-%m-%d %H:%M:%S')
        diff = str(end - start)

        return diff

    log = open_file("log_201503.h5", mode = "w")
    table = log.create_table('/', 'files', Particle)        
    row = table.row

    for name in fnames:
        print('processing', name)
        h5_path = "../../local_data/%s.h5" %(name[17:40])
        
        if not os.path.exists(h5_path):
            t0 = time.time()
            test = TAQ2Chunks(name, do_process_chunk=True)
            test.to_hdf5()
            t1 = time.time()

            row['name'] = name[32:40]
            row['time'] = timing(t0, t1)
            row.append()
