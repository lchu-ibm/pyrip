import os
import rasterio
from rasterio.windows import Window, transform
from rasterio.io import MemoryFile
from .util import run_command


def get_bounds(file):
    if isinstance(file, bytes):
        return MemoryFile(file).open().bounds
    return rasterio.open(file).bounds


def get_shape(file):
    if isinstance(file, bytes):
        return MemoryFile(file).open().shape
    return rasterio.open(file).shape


def rescale(infile, outfile, src_min, src_max, dst_min=0, dst_max=255, dtype=None):
    args = ['gdal_translate']
    if dtype is not None:
        args.extend(['-ot', dtype])
    args.extend(['-scale', src_min, src_max, dst_min, dst_max, infile, outfile])
    run_command(args)
    return outfile


def merge(files, outfile, bbox=None, init_val=None, ignore_val=None, nodata_val=None):
    try:
        os.remove(outfile)
    except OSError:
        pass
    args = ['gdal_merge.py', '-o', outfile]
    if init_val is not None:
        args.extend(['-init', init_val])
    if ignore_val is not None:
        args.extend(['-n', ignore_val])
    if nodata_val is not None:
        args.extend(['-a_nodata', nodata_val])
    else:
        nodata = rasterio.open(files[0]).nodata
        if nodata is not None:
            args.extend(['-a_nodata', nodata])
    if bbox is not None:
        left, bottom, right, top = bbox
        args.extend(['-ul_lr', left, top, right, bottom])
    args.extend(files)
    if not os.path.exists(os.path.dirname(os.path.abspath(outfile))):
        os.makedirs(os.path.dirname(os.path.abspath(outfile)))
    run_command(args)

    # # Copy existing band descriptions to the merged image
    # from osgeo import gdal
    # # Fix a bug in conda gdal: https://github.com/OSGeo/gdal/issues/1231
    # if ';' in os.environ["PATH"]:
    #     os.environ["PATH"] = os.environ["PATH"].split(';')[1]
    # descriptions = rasterio.open(files[0]).descriptions
    # ds = gdal.Open(outfile, gdal.GA_Update)
    # for band, desc in enumerate(descriptions, start=1):
    #     if desc is not None:
    #         rb = ds.GetRasterBand(band)
    #         rb.SetDescription(desc)
    # del ds

    return outfile


def split(infile, width, height=None):
    dataset = rasterio.open(infile)
    height = height or width
    raw_width = dataset.width
    raw_height = dataset.height
    raw_window = Window(0, 0, raw_width, raw_height)
    filename = dataset.name
    outfiles = []
    for col_off in range(0, raw_width, width):
        for row_off in range(0, raw_height, height):
            window = Window(col_off, row_off, width, height).intersection(raw_window)
            meta = dataset.meta.copy()
            meta.update({
                'width': window.width,
                'height': window.height,
                'transform': transform(window, dataset.transform)
            })
            outfile = os.path.splitext(filename)[0] + "_{}_{}_{}_{}".format(col_off, row_off, window.width, window.height) + os.path.splitext(filename)[1]
            with rasterio.open(outfile, 'w', **meta) as dst:
                dst.write(dataset.read(window=window))
                # Copy existing band descriptions to the splitted images
                descriptions = rasterio.open(infile).descriptions
                for band, desc in enumerate(descriptions, start=1):
                    if desc is not None:
                        dst.set_band_description(band, desc)
                outfiles.append(outfile)
    return outfiles


def query(infile, bbox, outfile=None):
    dataset = rasterio.open(infile)
    left, bottom, right, top = bbox
    start = dataset.index(left, top)
    stop = dataset.index(right, bottom)
    window = Window.from_slices((start[0], stop[0]), (start[1], stop[1]), boundless=True)
    meta = dataset.meta.copy()
    meta.update({
        'width': window.width,
        'height': window.height,
        'transform': transform(window, dataset.transform)
    })
    if outfile is None:
        filename = dataset.name
        outfile = os.path.splitext(filename)[0] + "_{}_{}_{}_{}".format(left, bottom, right, top) + os.path.splitext(filename)[1]
    with rasterio.open(outfile, 'w', **meta) as dst:
        dst.write(dataset.read(window=window, boundless=True))
        # Copy existing band descriptions to the splitted images
        descriptions = rasterio.open(infile).descriptions
        for band, desc in enumerate(descriptions, start=1):
            if desc is not None:
                dst.set_band_description(band, desc)
    return outfile
