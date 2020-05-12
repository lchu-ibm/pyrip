import os
import rasterio
import subprocess
from rasterio.windows import Window, transform
from rasterio.io import MemoryFile


def get_bounds(file):
    if isinstance(file, bytes):
        return MemoryFile(file).open().bounds
    return rasterio.open(file).bounds


def merge(files, outfile, bbox=None):
    args = ['gdal_merge.py', '-o', outfile]
    nodata = rasterio.open(files[0]).nodata
    if nodata is not None:
        args.extend(['-a_nodata', str(nodata)])
    if bbox is not None:
        left, bottom, right, top = bbox
        args.extend(['-ul_lr', str(left), str(top), str(right), str(bottom)])
    args.extend(files)
    if not os.path.exists(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)

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
