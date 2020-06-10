import numpy as np
import pandas as pd
import tempfile
import subprocess
import rasterio
import os
from pyrip.util import create_xyz_vrt
from pyrip.bands import stack_bands
import tempfile
import zipfile
from pathlib import Path

from osgeo import gdal
# Fix a bug in conda gdal: https://github.com/OSGeo/gdal/issues/1231
if ';' in os.environ["PATH"]:
    os.environ["PATH"] = os.environ["PATH"].split(';')[1]


# def tif_to_xyz_gdal(infile, outfile, band=1, sep=',', header=False):
#     args = ['gdal_translate', '-of', 'XYZ', infile, outfile]
#     if band != 1:
#         args.extend(['-b', str(band)])
#     if sep != ' ':
#         args.extend(['-co', 'COLUMN_SEPARATOR={}'.format(sep)])
#     if header:
#         args.extend(['-co', 'ADD_HEADER_LINE=YES'])
#     subprocess.run(args)
#     return outfile
#
#
# def tif_to_df_gdal(infile, drop_nodata=True):
#     dataset = rasterio.open(infile)
#     num_bands = dataset.count
#     if num_bands == 1:
#         nodata = dataset.nodata
#         with tempfile.NamedTemporaryFile() as tmpfile:
#             tif_to_xyz_gdal(infile, tmpfile.name)
#             df = pd.read_csv(tmpfile, header=None, names=['lon', 'lat', 'value'], na_values=nodata)
#             if drop_nodata:
#                 df.dropna(inplace=True)
#         return df
#     else:
#         dfs = []
#         for band in range(1, num_bands+1):
#             nodata = dataset.nodatavals[band-1]
#             desc = dataset.descriptions[band-1]
#             with tempfile.NamedTemporaryFile() as tmpfile:
#                 tif_to_xyz_gdal(infile, tmpfile.name, band=band)
#                 df = pd.read_csv(tmpfile, header=None, names=['lon', 'lat', 'value'], na_values=nodata)
#                 if drop_nodata:
#                     df.dropna(inplace=True)
#                 df['band_num'] = band
#                 df['band_desc'] = desc
#                 dfs.append(df)
#         return pd.concat(dfs)


# def tif_to_df(infile, drop_nodata=True):
#     import psutil
#     process = psutil.Process(os.getpid())
#
#     dataset = rasterio.open(infile)
#     a, b, c, d, e, f, g, _, _ = dataset.transform
#     affine_matrix = np.array([[a, b, c],
#                               [d, e, f],
#                               [0, 0, 1]])
#     print("xy_pixel")
#     nrows, ncols = dataset.shape
#     xy_pixel = np.stack((np.tile(np.arange(ncols), nrows) + 0.5,
#                          np.repeat(np.arange(nrows), ncols) + 0.5,
#                          np.repeat(1, ncols * nrows)))
#     print(process.memory_info().rss / 1000.0 / 1000 / 1000)
#     print("xy_multiple")
#     xy = np.matmul(affine_matrix, xy_pixel)
#     print(process.memory_info().rss / 1000.0 / 1000 / 1000)
#     print("df")
#     df = pd.DataFrame({'lon': xy[0], 'lat': xy[1], 'value': dataset.read(1).ravel()})
#     print(process.memory_info().rss / 1000.0 / 1000 / 1000)
#     if drop_nodata:
#         df = df[df['value'] != dataset.nodata]
#     # Cast unsigned int (uint8, uint16, uint32) to int32 as spark does not support unsigned int in parquet files
#     if 'uint' in dataset.dtypes[0].lower():
#         df['value'] = df['value'].astype('int32')
#     return df


def tif_to_df(infile, drop_nodata=True):
    dataset = rasterio.open(infile)
    value = dataset.read(1).ravel()
    a, b, c, d, e, f, g, _, _ = dataset.transform
    affine_matrix = np.array([[a, b, c],
                              [d, e, f],
                              [0, 0, 1]])
    nrows, ncols = dataset.shape
    xy_pixel = np.stack((np.tile(np.arange(ncols), nrows) + 0.5,
                         np.repeat(np.arange(nrows), ncols) + 0.5,
                         np.repeat(1, ncols * nrows)))
    if drop_nodata:
        mask = value != dataset.nodata
        value = value[mask]
        xy_pixel = xy_pixel[:, mask]
    xy = np.matmul(affine_matrix, xy_pixel)
    df = pd.DataFrame({'lon': xy[0], 'lat': xy[1], 'value': value})
    # Cast unsigned int (uint8, uint16, uint32) to int32 as spark does not support unsigned int in parquet files
    if 'uint' in dataset.dtypes[0].lower():
        df['value'] = df['value'].astype('int32')
    return df


def xyz_to_tif(infile, outfile, xres, yres=None, bbox=None, val_col='value', nodata=-9999, dtype='Float64'):
    yres = yres or xres
    vrt = create_xyz_vrt(infile)
    with tempfile.NamedTemporaryFile(suffix='.vrt') as tmpfile:
        vrt.write(tmpfile.name)
        args = ['gdal_rasterize', tmpfile.name, outfile]
        # layer name, currently hardcoded in util.create_xyz_vrt()
        # args.extend(['-l', os.path.splitext(os.path.basename(infile))[0]])
        # output format
        args.extend(['-of', 'GTiff'])
        # set nodata value
        args.extend(['-a_nodata', str(nodata)])
        # set output data type
        args.extend(['-ot', dtype])
        # attribute column used for burn-in value
        args.extend(['-a', val_col])
        # set target resolution in georeferenced units
        args.extend(['-tr', str(xres), str(yres)])
        # set output bounding box, leave none to compute boundary's bbox automatically
        if bbox is not None:
            xmin, ymin, xmax, ymax = bbox
            args.extend(['-te', str(xmin), str(ymin), str(xmax), str(ymax)])
        subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
    return outfile


def df_to_tif(df, outfile, xres, yres=None, bbox=None, val_col='value', nodata=-9999, dtype='Float64', band_col=None, band_desc_col=None):
    if band_col is None:
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmpfile:
            df.to_csv(tmpfile.name, index=False)
            return xyz_to_tif(tmpfile.name, outfile, xres, yres, bbox, val_col, nodata, dtype)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            bands = sorted(df[band_col].unique())
            tif_files = []
            for band in bands:
                csv_file = os.path.join(tmpdir, 'band_{}.csv'.format(band))
                tif_file = os.path.join(tmpdir, 'band_{}.tif'.format(band))
                tmp_df = df[df[band_col] == band]
                tmp_df.to_csv(csv_file, index=False)
                xyz_to_tif(csv_file, tif_file, xres, yres, bbox, val_col, nodata, dtype)
                # Set band description
                if band_desc_col is None:
                    band_desc = 'band_{}'.format(band)
                else:
                    band_desc = tmp_df[band_desc_col].iloc[0]
                ds = gdal.Open(tif_file, gdal.GA_Update)
                rb = ds.GetRasterBand(1)
                rb.SetDescription(band_desc)
                del ds
                tif_files.append(tif_file)
            return stack_bands(tif_files, outfile)


def hdf_to_tif(infile, outdir=None, match_substrs=None):
    ds = gdal.Open(infile, gdal.GA_ReadOnly)
    outfiles = []
    for sub_ds in ds.GetSubDatasets():
        sub_ds_name = sub_ds[0]
        sub_ds_layer_name = sub_ds_name.split(':')[-1]
        if match_substrs is not None and not any(match_substr in sub_ds_layer_name for match_substr in match_substrs):
            continue
        if outdir is None:
            outfile = os.path.splitext(infile)[0] + '.' + sub_ds_layer_name + '.tif'
        else:
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            outfilename = os.path.splitext(os.path.basename(infile))[0] + '.' + sub_ds_layer_name + '.tif'
            outfile = os.path.join(outdir, outfilename)
        args = ['gdal_translate', '-of', 'GTiff', sub_ds_name, outfile]
        subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
        outfiles.append(outfile)
    return outfiles


def safe_to_tif(infile, outdir=None, patterns=None):
    outdir = outdir or str(Path(infile).resolve().parent)
    patterns = patterns or ['*.jp2']
    # if .SAFE dir:
    if os.path.isdir(infile):
        outfiles = []
        jp2_files = []
        for pattern in patterns:
            jp2_files.extend(Path(infile).rglob(pattern))
        for jp2_file in jp2_files:
            outfile = os.path.join(outdir, os.path.splitext(os.path.basename(str(jp2_file)))[0] + '.tif')
            args = ['gdal_translate', '-of', 'GTiff', str(jp2_file), outfile]
            subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
            outfiles.append(outfile)
        return outfiles
    elif zipfile.is_zipfile(infile):
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(infile) as zip_ref:
                zip_ref.extractall(tmpdir)
            return safe_to_tif(tmpdir, outdir, patterns)
    else:
        raise TypeError("infile must be either a directory or zip file")


def grib_to_tif(infile, outfile=None):
    outfile = outfile or os.path.splitext(infile)[0] + '.tif'
    args = ['gdal_translate', '-of', 'GTiff', '-ot', 'Byte', '-scale', infile, outfile]
    try:
        subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    except subprocess.CalledProcessError as e:
        raise ValueError("""
        Command Failed: {}
        
        STDOUT: {}
        
        STDERR: {}
        """.format(' '.join(e.cmd), e.stdout.decode(), e.stderr.decode()))
    return outfile
