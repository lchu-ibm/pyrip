import os
import rasterio
from rasterio.warp import calculate_default_transform, Resampling, reproject as rasterio_reproject
from .util import run_command


# def reproject(infile, outfile=None, t_srs='EPSG:4326'):
#     outfile = outfile or os.path.splitext(infile)[0] + '_' + t_srs.replace(':', '') + os.path.splitext(infile)[1]
#     subprocess.run(['gdalwarp', '-t_srs', t_srs,'-overwrite', infile, outfile], check=True, stdout=subprocess.DEVNULL)
#     return outfile


def reproject(infile, outfile=None, dst_crs='EPSG:4326'):
    outfile = outfile or os.path.splitext(infile)[0] + '_' + dst_crs.replace(':', '') + os.path.splitext(infile)[1]
    with rasterio.open(infile) as src:
        with rasterio.Env(CHECK_WITH_INVERT_PROJ=True):
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height
        })
        with rasterio.open(outfile, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                rasterio_reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest)
    return outfile


def warp(infile, outfile, bounds, resolution=None, shape=None, align_pixels=False):
    outfile = outfile or os.path.splitext(infile)[0] + '_warped' + os.path.splitext(infile)[1]
    args = ['gdalwarp']
    args.extend(['-te', *bounds])
    if resolution is None and shape is None:
        raise ValueError('either resolution or shape must to be set.')
    elif resolution is not None and shape is not None:
        raise ValueError('resolution and shape cannot be set at the same time.')
    elif resolution is not None:
        args.extend(['-tr', resolution, resolution])
        if align_pixels:
            args.append('-tap')
    else:
        args.extend(['-ts', shape[1], shape[0]])
    args.extend(['-overwrite', infile, outfile])
    run_command(args)
    return outfile


def align(image, base_image, outfile=None):
    outfile = outfile or os.path.splitext(image)[0] + '_aligned' + os.path.splitext(image)[1]
    with rasterio.open(base_image) as dataset:
        bounds = dataset.bounds
        shape = dataset.shape
    return warp(image, outfile, bounds, shape=shape)

