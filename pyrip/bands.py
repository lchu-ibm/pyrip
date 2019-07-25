import os
import subprocess
import rasterio

from osgeo import gdal
# Fix a bug in conda gdal: https://github.com/OSGeo/gdal/issues/1231
if ';' in os.environ["PATH"]:
    os.environ["PATH"] = os.environ["PATH"].split(';')[1]


def stack_bands(files, outfile):
    nodata = rasterio.open(files[0]).nodata
    if nodata is not None:
        subprocess.run(['gdal_merge.py', '-separate', '-a_nodata', str(nodata), '-o', outfile, *files], check=True, stdout=subprocess.DEVNULL)
    else:
        subprocess.run(['gdal_merge.py', '-separate', '-o', outfile, *files], check=True, stdout=subprocess.DEVNULL)

    # Copy existing band descriptions to the merged image
    descriptions = ()
    for file in files:
        descriptions += rasterio.open(file).descriptions
    ds = gdal.Open(outfile, gdal.GA_Update)
    for band, desc in enumerate(descriptions, start=1):
        if desc is not None:
            rb = ds.GetRasterBand(band)
            rb.SetDescription(desc)
    del ds

    return outfile


def extract_bands(file, bands=None):
    dataset = rasterio.open(file)
    if bands is None:
        bands = range(1, dataset.count + 1)
    elif isinstance(bands, int):
        bands = [bands]
    meta = dataset.meta.copy()
    meta.update({
        'count': 1
    })
    outfiles = []
    descriptions = rasterio.open(file).descriptions
    for band in bands:
        outfile = os.path.splitext(file)[0] + "_band_{}".format(band) + os.path.splitext(file)[1]
        with rasterio.open(outfile, 'w', **meta) as dst:
            dst.write(dataset.read(band), 1)
            if descriptions[band - 1] is not None:
                dst.set_band_description(1, descriptions[band-1])
        outfiles.append(outfile)
    return outfiles
