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
