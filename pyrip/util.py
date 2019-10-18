import os
import xml.etree.ElementTree as ET


def create_xyz_vrt(infile):
    layer_name = os.path.splitext(os.path.basename(infile))[0]

    data_source = ET.Element('OGRVRTDataSource')
    layer = ET.SubElement(data_source, 'OGRVRTLayer', name=layer_name)
    ET.SubElement(layer, 'SrcDataSource').text = infile
    ET.SubElement(layer, 'GeometryType').text = 'wkbPoint'
    ET.SubElement(layer, 'LayerSRS').text = 'EPSG:4326'
    ET.SubElement(layer, 'GeometryField', encoding='PointFromColumns', x='lon', y='lat')
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
