import os
from .util import run_command


def fill_nodata(infile, outfile=None, inplace=True, max_distance=None):
    if outfile is not None and inplace:
        raise ValueError('outfile cannot be set with inplace=True. When inplace=True, interpolation will be performed '
                         'in place on the source file and no new file will be generated. Set inplace=False and specify '
                         'outfile if you need a new file for the interpolated result')
    args = ['gdal_fillnodata.py']
    if max_distance is not None:
        args.extend(['-md', max_distance])
    if inplace:
        args.append(infile)
    else:
        outfile = outfile or os.path.splitext(infile)[0] + '_filled' + os.path.splitext(infile)[1]
        args.extend([infile, outfile])
    run_command(args)
    return infile if inplace else outfile
