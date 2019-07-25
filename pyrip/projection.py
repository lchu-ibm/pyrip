import os
import rasterio
from rasterio.warp import calculate_default_transform, Resampling, reproject as rasterio_reproject
# import subprocess


# def reproject(infile, outfile=None, t_srs='EPSG:4326'):
#     outfile = outfile or os.path.splitext(infile)[0] + '_' + t_srs.replace(':', '') + os.path.splitext(infile)[1]
#     subprocess.run(['gdalwarp', infile, outfile, '-t_srs', t_srs], check=True, stdout=subprocess.DEVNULL)
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
