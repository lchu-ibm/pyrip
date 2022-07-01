import gzip
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from osgeo import gdal

from .band import stack_bands
from .util import create_xyz_vrt, run_command, bake_gdal_options

# Fix a bug in conda gdal: https://github.com/OSGeo/gdal/issues/1231
if ';' in os.environ["PATH"]:
    os.environ["PATH"] = os.environ["PATH"].split(';')[1]


def tif_to_numpy(file):
    with rasterio.open(file) as dataset:
        if dataset.count == 1:
            return dataset.read(1)
        else:
            return dataset.read()


def tif_to_df(infile, drop_nodata=True):
    dataset = rasterio.open(infile)
    nrows, ncols = dataset.shape
    # if nrows * ncols > split_size * split_size:
    #     dfs = []
    #     tif_files = split(infile, split_size)
    #     for tif_file in tif_files:
    #         dfs.append(tif_to_df(tif_file, drop_nodata, split_size))
    #     return pd.concat(dfs)
    value = dataset.read(1).ravel()
    a, b, c, d, e, f, g, _, _ = dataset.transform
    affine_matrix = np.array([[a, b, c],
                              [d, e, f],
                              [0, 0, 1]])
    xy_pixel = np.stack((np.tile(np.arange(ncols), nrows) + 0.5,
                         np.repeat(np.arange(nrows), ncols) + 0.5,
                         np.repeat(1, ncols * nrows)))
    if drop_nodata:
        mask = value != dataset.nodata
        value = value[mask]
        xy_pixel = xy_pixel[:, mask]
    xy = np.matmul(affine_matrix, xy_pixel)
    df = pd.DataFrame({'lat': xy[1], 'lon': xy[0], 'value': value})
    # Cast unsigned int (uint8, uint16, uint32) to int32 as spark does not support unsigned int in parquet files
    if 'uint' in dataset.dtypes[0].lower():
        df['value'] = df['value'].astype('int32')
    return df


def xyz_to_tif(infile, outfile, xres, yres=None, bbox=None, lat_col='lat', lon_col='lon', val_col='value', nodata=-9999, dtype='Float64'):
    yres = yres or xres
    vrt = create_xyz_vrt(infile, lat_col, lon_col)
    with tempfile.NamedTemporaryFile(suffix='.vrt') as tmpfile:
        vrt.write(tmpfile.name)
        args = ['gdal_rasterize', tmpfile.name, outfile]
        # layer name, currently hardcoded in util.create_xyz_vrt()
        # args.extend(['-l', os.path.splitext(os.path.basename(infile))[0]])
        # output format
        args.extend(['-of', 'GTiff'])
        # set nodata value
        args.extend(['-a_nodata', nodata])
        # set output data type
        args.extend(['-ot', dtype])
        # attribute column used for burn-in value
        args.extend(['-a', val_col])
        # set target resolution in georeferenced units
        args.extend(['-tr', xres, yres])
        # set output bounding box, leave none to compute boundary's bbox automatically
        if bbox is not None:
            xmin, ymin, xmax, ymax = bbox
            args.extend(['-te', xmin, ymin, xmax, ymax])
        run_command(args)
    return outfile


def df_to_tif(df, outfile, xres, yres=None, bbox=None, lat_col='lat', lon_col='lon', val_col='value', nodata=-9999, dtype='Float64', band_col=None, band_desc_col=None):
    if band_col is None:
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmpfile:
            df.to_csv(tmpfile.name, index=False)
            return xyz_to_tif(tmpfile.name, outfile, xres, yres, bbox, lat_col, lon_col, val_col, nodata, dtype)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            bands = sorted(df[band_col].unique())
            tif_files = []
            for band in bands:
                csv_file = os.path.join(tmpdir, 'band_{}.csv'.format(band))
                tif_file = os.path.join(tmpdir, 'band_{}.tif'.format(band))
                tmp_df = df[df[band_col] == band]
                tmp_df.to_csv(csv_file, index=False)
                xyz_to_tif(csv_file, tif_file, xres, yres, bbox, lat_col, lon_col, val_col, nodata, dtype)
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


def hdf_to_tif(infile, outdir=None, match_substrs=None, gdal_options=None):
    gdal_options = bake_gdal_options(gdal_options)
    if infile.endswith('.gz'):
        decompressed_file = os.path.splitext(infile)[0]
        with gzip.open(infile) as _infile:
            with open(decompressed_file, 'wb') as _outfile:
                shutil.copyfileobj(_infile, _outfile)
        return hdf_to_tif(decompressed_file, outdir, match_substrs)
    ds = gdal.Open(infile, gdal.GA_ReadOnly)
    outfiles = []
    # if hdf/netCDF file contains only one image, then it should be accessed directly
    if not ds.GetSubDatasets():
        outfile = os.path.splitext(infile)[0] + '.tif'
        if outdir:
            outfile = os.path.join(outdir, outfile)
        args = ['gdal_translate', '-of', 'GTiff'] + gdal_options + [infile, outfile]
        run_command(args)
        outfiles.append(outfile)
    # if hdf/netCDF file contains more than one images (datasets):
    else:
        for sub_ds in ds.GetSubDatasets():
            sub_ds_name = sub_ds[0]
            sub_ds_layer_name = sub_ds_name.split(':')[-1]
            if match_substrs is not None and not any(match_substr in sub_ds_layer_name for match_substr in match_substrs):
                continue
            basename = os.path.splitext(os.path.basename(infile))[0]
            connector = '_' if basename.count('_') >= basename.count('.') else '.'
            if outdir is None:
                outfile = os.path.splitext(infile)[0] + connector + sub_ds_layer_name + '.tif'
            else:
                if not os.path.exists(outdir):
                    os.makedirs(outdir)
                outfilename = os.path.splitext(os.path.basename(infile))[0] + connector + sub_ds_layer_name + '.tif'
                outfile = os.path.join(outdir, outfilename)
            args = ['gdal_translate', '-of', 'GTiff'] + gdal_options + [sub_ds_name, outfile]
            run_command(args)
            outfiles.append(outfile)
    return outfiles


nc_to_tif = hdf_to_tif
netcdf_to_tif = hdf_to_tif


def safe_to_tif(infile, outdir=None, patterns=None, gdal_options=None):
    outdir = outdir or str(Path(infile).resolve().parent)
    patterns = patterns or ['*.jp2']
    gdal_options = bake_gdal_options(gdal_options)
    # if .SAFE dir:
    if os.path.isdir(infile):
        outfiles = []
        jp2_files = []
        for pattern in patterns:
            jp2_files.extend(Path(infile).rglob(pattern))
        for jp2_file in jp2_files:
            outfile = os.path.join(outdir, os.path.splitext(os.path.basename(str(jp2_file)))[0] + '.tif')
            args = ['gdal_translate', '-of', 'GTiff'] + gdal_options + [str(jp2_file), outfile]
            run_command(args)
            outfiles.append(outfile)
        return outfiles
    elif zipfile.is_zipfile(infile):
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(infile) as zip_ref:
                zip_ref.extractall(tmpdir)
            return safe_to_tif(tmpdir, outdir, patterns)
    else:
        raise TypeError("infile must be either a directory or zip file")


def grib_to_tif(infile, outfile=None, gdal_options=None):
    if infile.endswith('.gz'):
        decompressed_file = os.path.splitext(infile)[0]
        with gzip.open(infile) as _infile:
            with open(decompressed_file, 'wb') as _outfile:
                shutil.copyfileobj(_infile, _outfile)
        return grib_to_tif(decompressed_file, outfile, gdal_options)
    gdal_options = bake_gdal_options(gdal_options)
    outfile = outfile or os.path.splitext(infile)[0] + '.tif'
    args = ['gdal_translate', '-of', 'GTiff', '-ot', 'Byte', '-scale'] + gdal_options + [infile, outfile]
    run_command(args)
    return outfile


def grib2_to_tif(infile, outfile=None, gdal_options=None):
    if infile.endswith('.gz'):
        decompressed_file = os.path.splitext(infile)[0]
        with gzip.open(infile) as _infile:
            with open(decompressed_file, 'wb') as _outfile:
                shutil.copyfileobj(_infile, _outfile)
        return grib2_to_tif(decompressed_file, outfile, gdal_options)
    gdal_options = bake_gdal_options(gdal_options)
    outfile = outfile or os.path.splitext(infile)[0] + '.tif'
    args = ['gdal_translate', '-of', 'GTiff'] + gdal_options + [infile, outfile]
    run_command(args)
    return outfile


def jp2_to_tif(infile, outfile=None, gdal_options=None):
    if infile.endswith('.gz'):
        decompressed_file = os.path.splitext(infile)[0]
        with gzip.open(infile) as _infile:
            with open(decompressed_file, 'wb') as _outfile:
                shutil.copyfileobj(_infile, _outfile)
        return jp2_to_tif(decompressed_file, outfile, gdal_options)
    gdal_options = bake_gdal_options(gdal_options)
    outfile = outfile or os.path.splitext(infile)[0] + '.tif'
    args = ['gdal_translate', '-of', 'GTiff'] + gdal_options + [infile, outfile]
    run_command(args)
    return outfile
