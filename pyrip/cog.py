import os
import struct
from shapely.geometry import Point
from shapely.ops import transform
from shapely import wkt
import pyproj
from .util import run_command
import subprocess

from osgeo import gdal

# Fix a bug in conda gdal: https://github.com/OSGeo/gdal/issues/1231
if ';' in os.environ["PATH"]:
    os.environ["PATH"] = os.environ["PATH"].split(';')[1]


def optimize(infile, outfile=None, block_size=512, compression='LZW', predictor=2, big_tiff=True,
             access_key_id=None, secret_access_key=None, endpoint=None, **kwargs):
    outfile = outfile or os.path.splitext(infile)[0] + '_cog' + os.path.splitext(infile)[1]
    args = ['gdal_translate']
    args.extend(['-co', 'TILED=YES'])
    args.extend(['-co', 'BLOCKXSIZE={}'.format(block_size)])
    args.extend(['-co', 'BLOCKYSIZE={}'.format(block_size)])
    args.extend(['-co', 'COMPRESS={}'.format(compression.upper() if compression else 'NONE')])
    args.extend(['-co', 'PREDICTOR={}'.format(predictor)])
    args.extend(['-co', 'COPY_SRC_OVERVIEWS=YES'])
    args.extend(['-co', 'BIGTIFF={}'.format('YES' if big_tiff else 'NO')])
    for k, v in kwargs.items():
        args.extend(['-co', '{}={}'.format(k, v)])

    if infile.startswith('/vsis3'):
        if access_key_id is None or secret_access_key is None or endpoint is None:
            raise ValueError(
                'access_key_id, secret_access_key and endpoint must be set when infile is from cloud storage')
        args.extend(['--config', 'AWS_ACCESS_KEY_ID', access_key_id])
        args.extend(['--config', 'AWS_SECRET_ACCESS_KEY', secret_access_key])
        args.extend(['--config', 'AWS_S3_ENDPOINT', endpoint.replace('https://', '')])
        args.extend(['--config', 'GDAL_HTTP_UNSAFESSL', 'YES'])

    args.extend([infile, outfile])
    run_command(args)
    return outfile


def build_vrt(outfile, infiles, resolution=None, access_key_id=None, secret_access_key=None, endpoint=None):
    args = ['gdalbuildvrt']

    # https://gdal.org/programs/gdalbuildvrt.html#cmdoption-gdalbuildvrt-resolution
    if resolution:
        args.extend(['-resolution', 'user', '-tr', resolution, resolution, '-tap'])

    if any(f.startswith('/vsis3') for f in infiles) or outfile.startswith('/vsis3'):
        if access_key_id is None or secret_access_key is None or endpoint is None:
            raise ValueError('access_key_id, secret_access_key and endpoint must be set for cloud reading')
        args.extend(['--config', 'AWS_ACCESS_KEY_ID', access_key_id])
        args.extend(['--config', 'AWS_SECRET_ACCESS_KEY', secret_access_key])
        args.extend(['--config', 'AWS_S3_ENDPOINT', endpoint.replace('https://', '')])
        args.extend(['--config', 'GDAL_HTTP_UNSAFESSL', 'YES'])

    args.append(outfile)
    if isinstance(infiles, str):
        infiles = [infiles]
    args.extend(infiles)
    run_command(args)
    return outfile


def query(infile, outfile, bbox=None, compression='LZW', bbox_srs='EPSG:4326', verbose=False,
          access_key_id=None, secret_access_key=None, endpoint=None):
    args = ['gdal_translate']
    args.extend(['-projwin_srs', bbox_srs])
    if bbox is not None:
        min_x, min_y, max_x, max_y = bbox
        args.extend(['-projwin', min_x, max_y, max_x, min_y])
    args.extend(['-co', 'COMPRESS={}'.format(compression.upper() if compression else 'NONE')])

    if infile.startswith('/vsis3'):
        if access_key_id is None or secret_access_key is None or endpoint is None:
            raise ValueError(
                'access_key_id, secret_access_key and endpoint must be set when infile is from cloud storage')
        args.extend(['--config', 'AWS_ACCESS_KEY_ID', access_key_id])
        args.extend(['--config', 'AWS_SECRET_ACCESS_KEY', secret_access_key])
        args.extend(['--config', 'AWS_S3_ENDPOINT', endpoint.replace('https://', '')])
        args.extend(['--config', 'GDAL_HTTP_UNSAFESSL', 'YES'])

    if verbose:
        args.extend(['--debug', 'on'])

    args.extend([infile, outfile])
    args = [str(arg) for arg in args]
    try:
        p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        if verbose:
            print(p.stderr.decode())
    except subprocess.CalledProcessError as e:
        raise ValueError("""
                Command Failed: {}

                STDOUT: {}

                STDERR: {}
                """.format(' '.join(e.cmd), e.stdout.decode(), e.stderr.decode()))
    return outfile


def buffer_point(lat, lon, radius):
    project = pyproj.Transformer.from_proj(
        pyproj.Proj('+proj=aeqd +lat_0={} +lon_0={} +x_0=0 +y_0=0'.format(lat, lon)),
        pyproj.Proj('+proj=longlat +datum=WGS84')
    )
    return transform(project.transform, Point(0, 0).buffer(radius))


def query_radius(infile, outfile, lat, lon, radius, compression='LZW', bbox_srs='EPSG:4326', verbose=False,
                 access_key_id=None, secret_access_key=None, endpoint=None):
    return query(infile, outfile, buffer_point(lat, lon, radius).bounds, compression, bbox_srs, verbose,
                 access_key_id, secret_access_key, endpoint)


def query_polygon(infile, outfile, poly_wkt, compression='LZW', bbox_srs='EPSG:4326', verbose=False,
                 access_key_id=None, secret_access_key=None, endpoint=None):
    return query(infile, outfile, wkt.loads(poly_wkt).bounds, compression, bbox_srs, verbose,
                 access_key_id, secret_access_key, endpoint)


def check_info(file, access_key_id=None, secret_access_key=None, endpoint=None):
    args = ['gdalinfo', file]
    if file.startswith('/vsis3'):
        if access_key_id is None or secret_access_key is None or endpoint is None:
            raise ValueError(
                'access_key_id, secret_access_key and endpoint must be set when file is from cloud storage')
        args.extend(['--config', 'AWS_ACCESS_KEY_ID', access_key_id])
        args.extend(['--config', 'AWS_SECRET_ACCESS_KEY', secret_access_key])
        args.extend(['--config', 'AWS_S3_ENDPOINT', endpoint.replace('https://', '')])
        args.extend(['--config', 'GDAL_HTTP_UNSAFESSL', 'YES'])
    args = [str(arg) for arg in args]
    try:
        p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        print(p.stdout.decode())
    except subprocess.CalledProcessError as e:
        raise ValueError("""
            Command Failed: {}

            STDOUT: {}

            STDERR: {}
            """.format(' '.join(e.cmd), e.stdout.decode(), e.stderr.decode()))


def validate(filename, quiet=False, full_check=None):
    if full_check is None:
        full_check = filename.startswith('/vsimem/') or os.path.exists(filename)

    try:
        ret = True
        warnings, errors, details = _validate(filename, full_check=full_check)
        if warnings:
            if not quiet:
                print('The following warnings were found:')
                for warning in warnings:
                    print(' - ' + warning)
                print('')
        if errors:
            if not quiet:
                print('%s is NOT a valid cloud optimized GeoTIFF.' % filename)
                print('The following errors were found:')
                for error in errors:
                    print(' - ' + error)
                print('')
            ret = False
        else:
            if not quiet:
                print('%s is a valid cloud optimized GeoTIFF' % filename)

        if not quiet and not warnings and not errors:
            headers_size = min(details['data_offsets'][k] for k in details['data_offsets'])
            if headers_size == 0:
                headers_size = gdal.VSIStatL(filename).size
            print('\nThe size of all IFD headers is %d bytes' % headers_size)
    except ValidateCloudOptimizedGeoTIFFException as e:
        if not quiet:
            print('%s is NOT a valid cloud optimized GeoTIFF : %s' %
                  (filename, str(e)))
        ret = False

    return ret


def _validate(ds, check_tiled=True, full_check=False):
    """Check if a file is a (Geo)TIFF with cloud optimized compatible structure.

    Args:
      ds: GDAL Dataset for the file to inspect.
      check_tiled: Set to False to ignore missing tiling.
      full_check: Set to TRUe to check tile/strip leader/trailer bytes. Might be slow on remote files

    Returns:
      A tuple, whose first element is an array of error messages
      (empty if there is no error), and the second element, a dictionary
      with the structure of the GeoTIFF file.

    Raises:
      ValidateCloudOptimizedGeoTIFFException: Unable to open the file or the
        file is not a Tiff.
    """

    if int(gdal.VersionInfo('VERSION_NUM')) < 2020000:
        raise ValidateCloudOptimizedGeoTIFFException(
            'GDAL 2.2 or above required')

    unicode_type = type(''.encode('utf-8').decode('utf-8'))
    if isinstance(ds, (str, unicode_type)):
        gdal.PushErrorHandler()
        ds = gdal.Open(ds)
        gdal.PopErrorHandler()
        if ds is None:
            raise ValidateCloudOptimizedGeoTIFFException(
                'Invalid file : %s' % gdal.GetLastErrorMsg())
        if ds.GetDriver().ShortName != 'GTiff':
            raise ValidateCloudOptimizedGeoTIFFException(
                'The file is not a GeoTIFF')

    details = {}
    errors = []
    warnings = []
    filename = ds.GetDescription()
    main_band = ds.GetRasterBand(1)
    ovr_count = main_band.GetOverviewCount()
    filelist = ds.GetFileList()
    if filelist is not None and filename + '.ovr' in filelist:
        errors += [
            'Overviews found in external .ovr file. They should be internal']

    if main_band.XSize > 512 or main_band.YSize > 512:
        if check_tiled:
            block_size = main_band.GetBlockSize()
            if block_size[0] == main_band.XSize and block_size[0] > 1024:
                errors += [
                    'The file is greater than 512xH or Wx512, but is not tiled']

        if ovr_count == 0:
            warnings += [
                'The file is greater than 512xH or Wx512, it is recommended '
                'to include internal overviews']

    ifd_offset = int(main_band.GetMetadataItem('IFD_OFFSET', 'TIFF'))
    ifd_offsets = [ifd_offset]

    block_order_row_major = False
    block_leader_size_as_uint4 = False
    block_trailer_last_4_bytes_repeated = False
    mask_interleaved_with_imagery = False

    if ifd_offset not in (8, 16):

        # Check if there is GDAL hidden structural metadata
        f = gdal.VSIFOpenL(filename, 'rb')
        if not f:
            raise ValidateCloudOptimizedGeoTIFFException("Cannot open file")
        signature = struct.unpack('B' * 4, gdal.VSIFReadL(4, 1, f))
        bigtiff = signature in ((0x49, 0x49, 0x2B, 0x00), (0x4D, 0x4D, 0x00, 0x2B))
        if bigtiff:
            expected_ifd_pos = 16
        else:
            expected_ifd_pos = 8
        gdal.VSIFSeekL(f, expected_ifd_pos, 0)
        pattern = "GDAL_STRUCTURAL_METADATA_SIZE=%06d bytes\n" % 0
        got = gdal.VSIFReadL(len(pattern), 1, f).decode('LATIN1')
        if len(got) == len(pattern) and got.startswith('GDAL_STRUCTURAL_METADATA_SIZE='):
            size = int(got[len('GDAL_STRUCTURAL_METADATA_SIZE='):][0:6])
            extra_md = gdal.VSIFReadL(size, 1, f).decode('LATIN1')
            block_order_row_major = 'BLOCK_ORDER=ROW_MAJOR' in extra_md
            block_leader_size_as_uint4 = 'BLOCK_LEADER=SIZE_AS_UINT4' in extra_md
            block_trailer_last_4_bytes_repeated = 'BLOCK_TRAILER=LAST_4_BYTES_REPEATED' in extra_md
            mask_interleaved_with_imagery = 'MASK_INTERLEAVED_WITH_IMAGERY=YES' in extra_md
            if 'KNOWN_INCOMPATIBLE_EDITION=YES' in extra_md:
                errors += ["KNOWN_INCOMPATIBLE_EDITION=YES is declared in the file"]
            expected_ifd_pos += len(pattern) + size
            expected_ifd_pos += expected_ifd_pos % 2  # IFD offset starts on a 2-byte boundary
        gdal.VSIFCloseL(f)

        if expected_ifd_pos != ifd_offsets[0]:
            errors += [
                'The offset of the main IFD should be %d. It is %d instead' % (expected_ifd_pos, ifd_offsets[0])]

    details['ifd_offsets'] = {}
    details['ifd_offsets']['main'] = ifd_offset

    for i in range(ovr_count):
        # Check that overviews are by descending sizes
        ovr_band = ds.GetRasterBand(1).GetOverview(i)
        if i == 0:
            if (ovr_band.XSize > main_band.XSize or
                    ovr_band.YSize > main_band.YSize):
                errors += [
                    'First overview has larger dimension than main band']
        else:
            prev_ovr_band = ds.GetRasterBand(1).GetOverview(i - 1)
            if (ovr_band.XSize > prev_ovr_band.XSize or
                    ovr_band.YSize > prev_ovr_band.YSize):
                errors += [
                    'Overview of index %d has larger dimension than '
                    'overview of index %d' % (i, i - 1)]

        if check_tiled:
            block_size = ovr_band.GetBlockSize()
            if block_size[0] == ovr_band.XSize and block_size[0] > 1024:
                errors += [
                    'Overview of index %d is not tiled' % i]

        # Check that the IFD of descending overviews are sorted by increasing
        # offsets
        ifd_offset = int(ovr_band.GetMetadataItem('IFD_OFFSET', 'TIFF'))
        ifd_offsets.append(ifd_offset)
        details['ifd_offsets']['overview_%d' % i] = ifd_offset
        if ifd_offsets[-1] < ifd_offsets[-2]:
            if i == 0:
                errors += [
                    'The offset of the IFD for overview of index %d is %d, '
                    'whereas it should be greater than the one of the main '
                    'image, which is at byte %d' %
                    (i, ifd_offsets[-1], ifd_offsets[-2])]
            else:
                errors += [
                    'The offset of the IFD for overview of index %d is %d, '
                    'whereas it should be greater than the one of index %d, '
                    'which is at byte %d' %
                    (i, ifd_offsets[-1], i - 1, ifd_offsets[-2])]

    # Check that the imagery starts by the smallest overview and ends with
    # the main resolution dataset

    def _get_block_offset(band):
        blockxsize, blockysize = band.GetBlockSize()
        for y in range(int((band.YSize + blockysize - 1) / blockysize)):
            for x in range(int((band.XSize + blockxsize - 1) / blockxsize)):
                block_offset = band.GetMetadataItem('BLOCK_OFFSET_%d_%d' % (x, y), 'TIFF')
                if block_offset:
                    return int(block_offset)
        return 0

    block_offset = _get_block_offset(main_band)
    data_offsets = [block_offset]
    details['data_offsets'] = {}
    details['data_offsets']['main'] = block_offset
    for i in range(ovr_count):
        ovr_band = ds.GetRasterBand(1).GetOverview(i)
        block_offset = _get_block_offset(ovr_band)
        data_offsets.append(block_offset)
        details['data_offsets']['overview_%d' % i] = block_offset

    if data_offsets[-1] != 0 and data_offsets[-1] < ifd_offsets[-1]:
        if ovr_count > 0:
            errors += [
                'The offset of the first block of the smallest overview '
                'should be after its IFD']
        else:
            errors += [
                'The offset of the first block of the image should '
                'be after its IFD']
    for i in range(len(data_offsets) - 2, 0, -1):
        if data_offsets[i] != 0 and data_offsets[i] < data_offsets[i + 1]:
            errors += [
                'The offset of the first block of overview of index %d should '
                'be after the one of the overview of index %d' %
                (i - 1, i)]
    if len(data_offsets) >= 2 and data_offsets[0] != 0 and data_offsets[0] < data_offsets[1]:
        errors += [
            'The offset of the first block of the main resolution image '
            'should be after the one of the overview of index %d' %
            (ovr_count - 1)]

    if full_check and (block_order_row_major or block_leader_size_as_uint4 or
                       block_trailer_last_4_bytes_repeated or
                       mask_interleaved_with_imagery):
        f = gdal.VSIFOpenL(filename, 'rb')
        if not f:
            raise ValidateCloudOptimizedGeoTIFFException("Cannot open file")

        _full_check_band(f, 'Main resolution image', main_band, errors,
                         block_order_row_major,
                         block_leader_size_as_uint4,
                         block_trailer_last_4_bytes_repeated,
                         mask_interleaved_with_imagery)
        if main_band.GetMaskFlags() == gdal.GMF_PER_DATASET and \
                (filename + '.msk') not in ds.GetFileList():
            _full_check_band(f, 'Mask band of main resolution image',
                             main_band.GetMaskBand(), errors,
                             block_order_row_major,
                             block_leader_size_as_uint4,
                             block_trailer_last_4_bytes_repeated, False)
        for i in range(ovr_count):
            ovr_band = ds.GetRasterBand(1).GetOverview(i)
            _full_check_band(f, 'Overview %d' % i, ovr_band, errors,
                             block_order_row_major,
                             block_leader_size_as_uint4,
                             block_trailer_last_4_bytes_repeated,
                             mask_interleaved_with_imagery)
            if ovr_band.GetMaskFlags() == gdal.GMF_PER_DATASET and \
                    (filename + '.msk') not in ds.GetFileList():
                _full_check_band(f, 'Mask band of overview %d' % i,
                                 ovr_band.GetMaskBand(), errors,
                                 block_order_row_major,
                                 block_leader_size_as_uint4,
                                 block_trailer_last_4_bytes_repeated, False)
        gdal.VSIFCloseL(f)

    return warnings, errors, details


def _full_check_band(f, band_name, band, errors,
                     block_order_row_major,
                     block_leader_size_as_uint4,
                     block_trailer_last_4_bytes_repeated,
                     mask_interleaved_with_imagery):
    block_size = band.GetBlockSize()
    mask_band = None
    if mask_interleaved_with_imagery:
        mask_band = band.GetMaskBand()
        mask_block_size = mask_band.GetBlockSize()
        if block_size != mask_block_size:
            errors += [band_name + ': mask block size is different from its imagery band']
            mask_band = None

    yblocks = (band.YSize + block_size[1] - 1) // block_size[1]
    xblocks = (band.XSize + block_size[0] - 1) // block_size[0]
    last_offset = 0
    for y in range(yblocks):
        for x in range(xblocks):

            offset = band.GetMetadataItem('BLOCK_OFFSET_%d_%d' % (x, y), 'TIFF')
            offset = int(offset) if offset is not None else 0
            bytecount = band.GetMetadataItem('BLOCK_SIZE_%d_%d' % (x, y), 'TIFF')
            bytecount = int(bytecount) if bytecount is not None else 0

            if offset > 0:
                if block_order_row_major and offset < last_offset:
                    errors += [band_name +
                               ': offset of block (%d, %d) is smaller than previous block' % (x, y)]

                if block_leader_size_as_uint4:
                    gdal.VSIFSeekL(f, offset - 4, 0)
                    leader_size = struct.unpack('<I', gdal.VSIFReadL(4, 1, f))[0]
                    if leader_size != bytecount:
                        errors += [band_name + ': for block (%d, %d), size in leader bytes is %d instead of %d' % (
                            x, y, leader_size, bytecount)]

                if block_trailer_last_4_bytes_repeated:
                    if bytecount >= 4:
                        gdal.VSIFSeekL(f, offset + bytecount - 4, 0)
                        last_bytes = gdal.VSIFReadL(8, 1, f)
                        if last_bytes[0:4] != last_bytes[4:8]:
                            errors += [band_name +
                                       ': for block (%d, %d), trailer bytes are invalid' % (x, y)]

            if mask_band:
                offset_mask = mask_band.GetMetadataItem('BLOCK_OFFSET_%d_%d' % (x, y), 'TIFF')
                offset_mask = int(offset_mask) if offset_mask is not None else 0
                if offset > 0 and offset_mask > 0:
                    # bytecount_mask = int(mask_band.GetMetadataItem('BLOCK_SIZE_%d_%d' % (x,y), 'TIFF'))
                    expected_offset_mask = offset + bytecount + \
                                           (4 if block_leader_size_as_uint4 else 0) + \
                                           (4 if block_trailer_last_4_bytes_repeated else 0)
                    if offset_mask != expected_offset_mask:
                        errors += [
                            'Mask of ' + band_name + ': for block (%d, %d), offset is %d, whereas %d was expected' % (
                                x, y, offset_mask, expected_offset_mask)]
                elif offset == 0 and offset_mask > 0:
                    if block_order_row_major and offset_mask < last_offset:
                        errors += ['Mask of ' + band_name +
                                   ': offset of block (%d, %d) is smaller than previous block' % (x, y)]

                    offset = offset_mask

            last_offset = offset


class ValidateCloudOptimizedGeoTIFFException(Exception):
    pass
