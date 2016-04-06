'''Mix up symbols and prices a little bit'''

from zipfile import ZipFile, ZIP_DEFLATED
from os import path

import taq
import taq.processing as tp

from shutil import copyfileobj


def main(fname_in, fname_out, size, frac):
    taq_in = taq.TAQ2Chunks(fname_in, do_process_chunk=False)
    downsampled = tp.downsample(taq_in, frac)
    sanitized = tp.Sanitizer(tp.split_chunks(downsampled, 'Symbol_root'))

    to_write = []
    writ_len = 0

    for chunk in sanitized:
        if len(chunk) + writ_len > size:
            break
        to_write.append(chunk)
        writ_len += len(chunk)

    line_len = len(taq_in.first_line)
    datestr, numlines = taq_in.first_line.split(b":")


    writ_str = str(writ_len)
    first_line = datestr + b':' + str(' '*4).encode() + str(writ_len).encode()
    first_line += str(' '*(line_len-len(first_line)-2)).encode() + b'\r\n'

    with open(fname_out, 'wb') as ofile:
        # write first line
        ofile.write(first_line)
        # write chunks from to_write
        for chunk in to_write:
            ofile.write(chunk)
            
    # new_line = 

    # tofile.write()

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
    parser.add_argument('size', type=int,
                        help="Integer number of lines to sanitize and write")
    parser.add_argument('--frac', '-f', type=float, default=0.001,
                        help='Floating point probability'
                             'of returning each line')
    args = parser.parse_args()

    main(args.fname_in, args.fname_out, args.size, args.frac)
