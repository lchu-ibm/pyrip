import os
import xml.etree.ElementTree as ET
import subprocess


def bake_gdal_options(gdal_options):
    if gdal_options is None:
        return []
    if not isinstance(gdal_options, list):
        raise ValueError("gdal_options must be a list, e.g. gdal_options=['-ot', 'Float32', '-scale']")
    gdal_options = [str(option) for option in gdal_options]
    return gdal_options


def run_command(args):
    args = [str(arg) for arg in args]
    try:
        subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    except subprocess.CalledProcessError as e:
        raise ValueError("""
            Command Failed: {}
            
            STDOUT: {}
            
            STDERR: {}
            """.format(' '.join(e.cmd), e.stdout.decode(), e.stderr.decode()))


def create_xyz_vrt(infile, lat_col='lat', lon_col='lon'):
    layer_name = os.path.splitext(os.path.basename(infile))[0]

    data_source = ET.Element('OGRVRTDataSource')
    layer = ET.SubElement(data_source, 'OGRVRTLayer', name=layer_name)
    ET.SubElement(layer, 'SrcDataSource').text = infile
    ET.SubElement(layer, 'GeometryType').text = 'wkbPoint'
    ET.SubElement(layer, 'LayerSRS').text = 'EPSG:4326'
    ET.SubElement(layer, 'GeometryField', encoding='PointFromColumns', x=lon_col, y=lat_col)
    tree = ET.ElementTree(data_source)
    return tree


def add_latitude(latitude, delta):
    return max(-90, min(90, latitude + delta))


def add_longitude(longitude, delta):
    result = longitude + delta
    while result > 180:
        result -= 360
    while result < -180:
        result += 360
    return result


def diff_longitude(start_longitude, end_longitude):
    diff = end_longitude - start_longitude
    if diff < 0:
        diff += 360
    return diff


def expand_bbox(bbox, delta):
    if diff_longitude(bbox[0], bbox[2]) + 2 * delta > 360:
        return -180,\
               add_latitude(bbox[1], -delta),\
               180,\
               add_latitude(bbox[3], delta)
    else:
        return add_longitude(bbox[0], -delta),\
               add_latitude(bbox[1], -delta),\
               add_longitude(bbox[2], delta),\
               add_latitude(bbox[3], delta)
