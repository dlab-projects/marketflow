import pandas as pd


class Permno_Map(object):
    """docstring for Permno_Map"""

    def __init__(self, dsefile='crsp/dsenames.csv'):
        self.dsenames = pd.read_csv(dsefile)
        # Once everything is working, perhaps make this automatic.
        # For now, it's easier to debug with smaller functions run one at a
        # time.
        # self.process(dsenames)

    def get_permno(self, root, date):
        '''Get the permno for a given symbol root.

        Remember, permno does not change with suffix.'''
        raise NotImplementedError

    def process(self, dsenames):
        '''Run all processing steps in a reasonable order'''
        dsenames = self.dse_subset(dsenames)
        dsenames = self.dse_rootsplit(dsenames)
        dsenames = self.drop_dups(dsenames)

        self.clean_dsenames = dsenames

    def dse_subset(self, dsenames, date=20100101, regular=True,
                   active=True, beneficial=False, when_issued=False):
        '''Limit to our "good" set of securities.

        date : int
            Not really an int, but the naïve conversion from the datestring.
        regular : bool
            Limit to "regular" stocks?
        active : bool
            Limit to "active" stocks?
        beneficial : bool
            XXX
        when_issued : bool
            XXX
        '''
        # By default, we only include securities that were trading at some
        # point from January 2010 onwards
        dsenames = dsenames[dsenames['NAMEENDT'] >= date]
        if regular:
            # SECSTAT == "R" indicates that the security is "Regular" (i.e. the
            # security is past the "When-Issued" stage and the company is not
            # going through bankruptcy proceedings)
            dsenames = dsenames[dsenames['SECSTAT'] == "R"]
        if active:
            # TRDSTAT == "A" indicates that the security is actively trading
            dsenames = dsenames[dsenames['TRDSTAT'] == "A"]

        dsenames['SYMBOL_LEN'] = dsenames['TSYMBOL'].str.len()
        dsenames['TICKER_LEN'] = dsenames['TICKER'].str.len()
        # There are 239 cases where the length of tsymbol does not match up
        # with the length of ticker For most of these cases, it is because
        # tsymbol includes a suffix of some sort, while ticker does not

        dsenames['LAST'] = dsenames['TSYMBOL'].str[-1]
        # This grabs the last two elements of tsymbol
        dsenames['LAST2'] = dsenames['TSYMBOL'].str[-2:]

        bad_permnos = [14209, 15141, 91845]
        # Here, we index out any securities whose records don't make sense
        dsenames = dsenames[~dsenames['PERMNO'].isin(bad_permnos)]

        # The 'S' suffix indicates the "Shares of Beneficial Interest, which do
        # not trade like regular securities The 'V' and 'WI' suffixes indicate
        # "When-Issued" shares, which have been authorized to trade, but have
        # not actually begun trading
        # XXX Maybe make this attribute defined at a "higher level"?
        self.nasdaq = dsenames.PRIMEXCH == "Q"
        beneficial = (dsenames['LAST'] == "S") & (dsenames['SYMBOL_LEN'] == 5)
        whenissued_nasdaq = ((dsenames['LAST'] == "V") &
                             (dsenames['SYMBOL_LEN'] == 5) & self.nasdaq)
        whenissued_nonnasdaq = ((dsenames['LAST2'] == "WI") &
                                (dsenames['SYMBOL_LEN'] > 3) & ~self.nasdaq)

        # Here, we take out the BoWI shares from bot NASDAQ and non-NASDAQ
        # securities
        dsenames = dsenames[~(beneficial & whenissued_nasdaq & whenissued_nonnasdaq)]

        return dsenames

    def dse_rootsplit(self, dsenames):
        '''XXX More docstrings!'''
        # Flag = 0 is our base case (i.e. the ticker symbol has no suffix)
        dsenames['FLAG'] = 0
        # When the ticker has no suffix, the root is just the ticker symbol, and the
        # suffix is an empty string
        dsenames['SYM_ROOT'] = dsenames['TSYMBOL']
        dsenames['SYM_SUFFIX'] = ""

        # Now begins the subsetting based on cases of symbol suffixes

        # Since we already have our nasdaq variable from earlier, we can create a few
        # more booleans to help us conduct the subsetting in a vectorized way

        # This first boolean vector will be True for a ticker symbol if its last
        # character is the same as its share class
        class_equal_last = dsenames.SHRCLS == dsenames.LAST

        # This boolean will be True for all NASDAQ securities with a ticker symbol
        # longer than 4 characters. 4 is the maximum number of characters for a ticker
        # symbol on the NASDAQ.
        nasdaq_long = self.nasdaq & (dsenames.SYMBOL_LEN > 4)

        # The first flag will denote securities with ticker symbols that have share
        # class suffixes, e.g. a company on the NASDAQ that has Class A and Class B
        # shares

        flag1 = nasdaq_long & class_equal_last

        # The second flag will denote two different special cases:
        # - Suffixes Y and F denote shares in foreign companies
        # - Suffixes J and K denote voting and non-voting shares, respectively
        flag2 = ~flag1 & nasdaq_long & dsenames.LAST.isin(["Y", "J", "F", "K"])

        # The third flag will denote (NASDAQ) stocks that are currently going through a
        # reverse stock split. These securities usually keep this ticker symbol for
        # about three weeks after the reverse stock split takes place.
        flag3 = ~flag1 & nasdaq_long & (dsenames.LAST == "D")

        # The fourth flag will denote non-NASDAQ stocks that have share class
        # suffixes.  For non-NASDAQ exchanges (e.g. the New York Stock
        # Exchange), ticker symbols without suffixes are 3 characters or less.
        flag4 = ~self.nasdaq & (dsenames.SYMBOL_LEN > 3) & class_equal_last

        # There is a fifth set of suffixed ticker symbols that do not fit into
        # the above categories, but they do have a unifying manual adjustment.
        # We denote this set as "funny" (not "funny" ha ha).

        funny_permnos = [85254, 29938, 29946, 93093, 92118, 83275, 82924,
                         82932, 77158, 46950, 90655]

        funny = (dsenames.PERMNO.isin(funny_permnos) &
                 (dsenames.SYMBOL_LEN - dsenames.TICKER_LEN == 1) &
                 dsenames.LAST.isin(["A", "B", "C", "S"])
                 )

        # Here, we assign the flags to each special case, as described above

        dsenames.loc[flag4, "FLAG"] = 4
        dsenames.loc[flag3, "FLAG"] = 3
        dsenames.loc[flag2, "FLAG"] = 2
        dsenames.loc[flag1, "FLAG"] = 1

        # Here, we group together the symboled suffixes to make the final
        # root-suffix separation cleaner. `sym5_with_suffix` is the set of
        # special cases with more than 4 characters in the symbol
        sym5_with_suffix = flag1 | flag2 | flag3
        symbol_with_suffix = flag4 | funny | sym5_with_suffix

        # Finally, the big enchilada, the separation of each ticker symbol into
        # its root and its symbol. Since we are only dealing with suffixes of
        # length 1, the root will consist of all but the last character, and
        # the root will be the ticker symbol's last character

        dsenames.loc[symbol_with_suffix, "SYM_ROOT"] = \
            dsenames.loc[symbol_with_suffix, "TSYMBOL"].str[0:-1]
        dsenames.loc[symbol_with_suffix, "SYM_SUFFIX"] = \
            dsenames.loc[symbol_with_suffix, "TSYMBOL"].str[-1]

        # There were a few wonky observations, so we do some additional manual
        # adjustments

        dsenames.loc[dsenames.PERMNO == 14461, "SYM_ROOT"] = \
            dsenames.loc[dsenames.PERMNO == 14461, "TSYMBOL"].str[0:-1]
        dsenames.loc[dsenames.PERMNO == 14461, "SYM_SUFFIX"] = \
            dsenames.loc[dsenames.PERMNO == 14461, "TSYMBOL"].str[-1]
        dsenames.loc[dsenames.PERMNO == 13914, "SYM_ROOT"] = \
            dsenames.loc[dsenames.PERMNO == 13914, "TSYMBOL"]
        dsenames.loc[dsenames.PERMNO == 13914, "SYM_SUFFIX"] = ""
        dsenames.loc[dsenames.PERMNO == 92895, "SYM_ROOT"] = "SAPX"
        dsenames.loc[dsenames.PERMNO == 92895, "SYM_SUFFIX"] = ""

        return dsenames

    def drop_dups(self, dsenames):
        '''XXX More docstrings!'''
        # Finally, we want to ensure that, when the same information is
        # recorded, the date range listed for the record reflects the entire
        # range over which the security was actively trading.

        # For instance, if a security stopped trading for a six month period,
        # it has two entries in this file. We want both of those entries to
        # include beginning date for the security's trading before the six
        # month break and the end date for the security's trading after the six
        # month break.

        # To do this, we first want to reset the index in the dsenames dataframe
        dsenames = dsenames.reset_index(drop=True)

        # When we say that we want to adjust the dates 'when the same
        # information is recorded,' we make that adjustment based on the
        # following seven variables in the data frame:
        # - Permno, the two components of the ticker symbol, the name of the
        # company the CUSIP number (current and historical), and
        # - the primary exchange on which the security trades

        # We first create a new data frame sorted on these 7 columns, which
        # only includes said 7 columns

        levels_sort = ['PERMNO', 'SYM_ROOT', 'SYM_SUFFIX', 'COMNAM', 'CUSIP',
                       'NCUSIP', 'PRIMEXCH']
        dsenames_sort = dsenames.sort_values(by=levels_sort).loc[:, levels_sort]
        dsenames = dsenames.sort_values(by=levels_sort)

        # We create two new variables, begdate and enddate, to capture the full
        # range of dates for which each security trades. The default case, when
        # a security only matches with itself based on the 7 sort levels, is
        # that the beginning date is the same as the beginning effective name
        # date, and the end date is the same as the end effective name date.

        dsenames['BEGDATE'] = dsenames.NAMEDT
        dsenames['ENDDATE'] = dsenames.NAMEENDT

        # We create a new dataframe that only includes the sort variables
        dsenames_sort_squish = dsenames_sort.loc[:, levels_sort]

        # Here, we create two copies of the dataframe:
        # 1. One without the first record, and
        # 2. one without the last
        dsenames_nofirst = dsenames_sort_squish.iloc[1:].reset_index(drop=True)
        dsenames_nolast = dsenames_sort_squish.iloc[:-1].reset_index(drop=True)

        # We then create a boolean matrix based on whether the entries of each
        # matrix match
        compare_matrix = (dsenames_nofirst == dsenames_nolast)

        # If the i-th record matches the next record for all 7 variables, then
        # the i-th row of the compare matrix will be all true. We extract the
        # index for subsetting purposes
        same_as_below = compare_matrix.all(axis=1)
        same_as_below_index = same_as_below.index[same_as_below]

        # In order to collapse the end dates, we will also need an index to
        # indicate if a record is the same as the one above.  This is simply
        # caputured by adding 1 to the first index we found
        same_as_above_index = same_as_below_index + 1

        # Finally, we loop through the first Int64Index we found to bring the
        # earliest `BEGDATE` for a record down to all of its matches.  Doing
        # this matching iteratively mitigates the issue of a particular
        # security having more than 2 records match based on the 7 variables.
        for i in same_as_above_index:
            dsenames['BEGDATE'].iat[i] = dsenames['BEGDATE'].iat[i-1]

        # Similar logic is used to bring the latest ENDDATE up - we just do it
        # backwards
        for i in same_as_below_index[::-1]:
            dsenames['ENDDATE'].iat[i] = dsenames['ENDDATE'].iat[i+1]

        # Finally, we output a final dataframe that includes only the columns
        # we sorted on and our new date variables. Since the same information
        # is recorded for these files now, we drop the duplicates
        final_columns = levels_sort + ['BEGDATE', 'ENDDATE']

        return dsenames.drop_duplicates(subset=final_columns).loc[:, final_columns]
