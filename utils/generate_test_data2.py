'''Mix up symbols and prices a little bit'''

from zipfile import ZipFile, ZIP_DEFLATED
from os import path

import taq
import taq.processing as tp


def main(fname_in, fname_out, size, frac):
    taq_in = taq.TAQ2Chunks(fname_in, do_process_chunk=False)
    downsampled = tp.Downsample(taq_in, frac)
    sanitized = tp.Sanitizer(tp.SplitChunks(downsampled, 'Symbol_root'))

    with open(fname_out, 'wb') as ofile:
        writ_len = 0
        ofile.write(taq_in.first_line)

        for chunk in sanitized:
            if len(chunk) + writ_len > size:
                break
            ofile.write(chunk)
            writ_len += len(chunk)

    basename = path.basename(fname_out)
    with ZipFile(fname_out + '.zip', 'w') as zf:
        zf.write(fname_out, basename, ZIP_DEFLATED)

    # Currently, the unzipped version of fname_out is left laying around!

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('fname_in', help="Path to Zipped TAQ data")
    parser.add_argument('fname_out', default='test_data_public',
                        help="Path to write output"
                             "(both zip archive and contained file")
    parser.add_argument('--size', type=int, default=10000,
                        help="Integer max of lines to sanitize and write")
    parser.add_argument('--frac', '-f', type=float, default=0.001,
                        help='Floating point probability'
                             'of returning each line')
    args = parser.parse_args()

    main(args.fname_in, args.fname_out, args.size, args.frac)
