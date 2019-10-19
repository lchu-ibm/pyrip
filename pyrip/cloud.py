import os
import six
import tempfile
from pyrip.util import expand_bbox


def download(cos_client, bucket, key, download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    cos_client.download_file(bucket, key, os.path.join(download_dir, os.path.basename(key)))
    return os.path.join(download_dir, os.path.basename(key))


def query_tiles_with_spark(spark, cos_api_key, cos_instance_crn, cos_endpoint_url, bucket, image_meta_prefix, layers,
                           start_date, end_date, bbox, download_dir, search_buffer_in_degrees=2, selected_dates=None,
                           merge_tiles=True, merge_by_date=True, force_bbox=True):

    if isinstance(layers, six.string_types):
        layers = [layers]

    if isinstance(selected_dates, six.string_types) or isinstance(selected_dates, six.integer_types):
        selected_dates = [selected_dates]

    # Get COS client for downloading tiles
    import ibm_boto3
    from ibm_botocore.client import Config
    cos_client = ibm_boto3.client("s3",
        ibm_api_key_id=cos_api_key,
        ibm_service_instance_id=cos_instance_crn,
        endpoint_url=cos_endpoint_url,
        config=Config(signature_version="oauth")
    )

    # Config Spark for reading meta data
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set('fs.cos.myCos.iam.api.key', cos_api_key)
    sc._jsc.hadoopConfiguration().set('fs.cos.myCos.iam.service.id', cos_instance_crn)
    sc._jsc.hadoopConfiguration().set('fs.cos.myCos.endpoint', cos_endpoint_url)

    # Get meta data
    spark.read.parquet('cos://{}.myCos/{}'.format(bucket, image_meta_prefix)).createOrReplaceTempView('temp_view')

    expanded_bbox = expand_bbox(bbox, search_buffer_in_degrees)

    if selected_dates is None:
        date_query_part = 'date between {} AND {}'.format(start_date, end_date)
    else:
        date_query_part = 'date in {}'.format(tuple(selected_dates))

    if expanded_bbox[2] >= expanded_bbox[0]:
        lon_query_part = 'lon between {} AND {}'.format(expanded_bbox[0], expanded_bbox[2])
    else:
        lon_query_part = '(lon between {} AND 180 OR lon between -180 AND {})'.format(expanded_bbox[0], expanded_bbox[2])

    df = spark.sql("""
    SELECT *
    FROM temp_view
    WHERE layer in {0}
    AND {1}
    AND lat between {2[1]} AND {2[3]}
    AND {3}
    """.format(tuple(layers), date_query_part, expanded_bbox, lon_query_part)).toPandas()

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    if merge_tiles:
        from pyrip.image import merge
        merged_images = []
        for layer in layers:
            tmp_df = df[df['layer'] == layer].sort_values(['date', 'lat', 'lon'])
            if merge_by_date:
                dates = tmp_df['date'].unique()
                for date in dates:
                    tiles = []
                    with tempfile.TemporaryDirectory() as tmpdir:
                        for url in tmp_df[tmp_df['date'] == date]['url']:
                            bucket, key = url.replace('cos://', '').split('/', 1)
                            tile = download(cos_client, bucket, key, tmpdir)
                            tiles.append(tile)
                        if bbox[2] > bbox[0]:
                            merged_image = os.path.join(download_dir, layer, str(date), 'merged.tif')
                            if force_bbox:
                                merge(tiles, merged_image, bbox)
                            else:
                                merge(tiles, merged_image)
                            merged_images.append(merged_image)
                        # If the given bbox spans over anti-meridian, we have to merge into two parts. Note that this
                        # will also overwrite the value of force_bbox and set it to true.
                        else:
                            merged_image_1 = os.path.join(download_dir, layer, str(date), 'merged_1.tif')
                            merged_image_2 = os.path.join(download_dir, layer, str(date), 'merged_2.tif')
                            merge(tiles, merged_image_1, (bbox[0], bbox[1], 180, bbox[3]))
                            merge(tiles, merged_image_2, (-180, bbox[1], bbox[2], bbox[3]))
                            merged_images.extend([merged_image_1, merged_image_2])
            else:
                tiles = []
                with tempfile.TemporaryDirectory() as tmpdir:
                    for url in tmp_df['url']:
                        bucket, key = url.replace('cos://', '').split('/', 1)
                        tile = download(cos_client, bucket, key, tmpdir)
                        tiles.append(tile)
                    if bbox[2] > bbox[0]:
                        merged_image = os.path.join(download_dir, layer, 'merged.tif')
                        if force_bbox:
                            merge(tiles, merged_image, bbox)
                        else:
                            merge(tiles, merged_image)
                        merged_images.append(merged_image)
                    # If the given bbox spans over anti-meridian, we have to merge into two parts. Note that this will
                    # also overwrite the value of force_bbox and set it to true.
                    else:
                        merged_image_1 = os.path.join(download_dir, layer, 'merged_1.tif')
                        merged_image_2 = os.path.join(download_dir, layer, 'merged_2.tif')
                        merge(tiles, merged_image_1, (bbox[0], bbox[1], 180, bbox[3]))
                        merge(tiles, merged_image_2, (-180, bbox[1], bbox[2], bbox[3]))
                        merged_images.extend([merged_image_1, merged_image_2])
        return merged_images
    else:
        tiles = []
        for _, row in df.iterrows():
            layer = row['layer']
            date = row['date']
            url = row['url']
            bucket, key = url.replace('cos://', '').split('/', 1)
            tile = download(cos_client, bucket, key, os.path.join(download_dir, layer, str(date)))
            tiles.append(tile)
        return tiles


def query_tiles(sql_client, cos_client, cos_url, target_cos_url, layers, start_date, end_date, bbox, download_dir,
                search_buffer_in_degrees=2, selected_dates=None, merge_tiles=True, merge_by_date=True, force_bbox=True):

    if isinstance(layers, six.string_types):
        layers = [layers]

    if isinstance(selected_dates, six.string_types) or isinstance(selected_dates, six.integer_types):
        selected_dates = [selected_dates]

    # By default, the result will be written into target_cos in CSV format. We disable this and make it write as
    # parquet to improvement performance
    if sql_client.target_cos is not None:
        sql_client.target_cos = None

    expanded_bbox = expand_bbox(bbox, search_buffer_in_degrees)

    if selected_dates is None:
        date_query_part = 'date between {} AND {}'.format(start_date, end_date)
    else:
        date_query_part = 'date in {}'.format(tuple(selected_dates))

    if expanded_bbox[2] >= expanded_bbox[0]:
        lon_query_part = 'lon between {} AND {}'.format(expanded_bbox[0], expanded_bbox[2])
    else:
        lon_query_part = '(lon between {} AND 180 OR lon between -180 AND {})'.format(expanded_bbox[0], expanded_bbox[2])

    query_str = """
    SELECT *
    FROM {0} STORED AS PARQUET
    WHERE layer in {1}
    AND {2}
    AND lat between {3[1]} AND {3[3]}
    AND {4}
    INTO {5} STORED AS parquet
    """.format(cos_url, tuple(layers), date_query_part, expanded_bbox, lon_query_part, target_cos_url)
    # ST_Contains(ST_WKTToSQL('BOUNDINGBOX({} {}, {} {})'), ST_Point(lon, lat))

    df = sql_client.run_sql(query_str)

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    if merge_tiles:
        from pyrip.image import merge
        merged_images = []
        for layer in layers:
            tmp_df = df[df['layer'] == layer].sort_values(['date', 'lat', 'lon'])
            if merge_by_date:
                dates = tmp_df['date'].unique()
                for date in dates:
                    tiles = []
                    with tempfile.TemporaryDirectory() as tmpdir:
                        for url in tmp_df[tmp_df['date'] == date]['url']:
                            bucket, key = url.replace('cos://', '').split('/', 1)
                            tile = download(cos_client, bucket, key, tmpdir)
                            tiles.append(tile)
                        if bbox[2] > bbox[0]:
                            merged_image = os.path.join(download_dir, layer, str(date), 'merged.tif')
                            if force_bbox:
                                merge(tiles, merged_image, bbox)
                            else:
                                merge(tiles, merged_image)
                            merged_images.append(merged_image)
                        # If the given bbox spans over anti-meridian, we have to merge into two parts. Note that this
                        # will also overwrite the value of force_bbox and set it to true.
                        else:
                            merged_image_1 = os.path.join(download_dir, layer, str(date), 'merged_1.tif')
                            merged_image_2 = os.path.join(download_dir, layer, str(date), 'merged_2.tif')
                            merge(tiles, merged_image_1, (bbox[0], bbox[1], 180, bbox[3]))
                            merge(tiles, merged_image_2, (-180, bbox[1], bbox[2], bbox[3]))
                            merged_images.extend([merged_image_1, merged_image_2])
            else:
                tiles = []
                with tempfile.TemporaryDirectory() as tmpdir:
                    for url in tmp_df['url']:
                        bucket, key = url.replace('cos://', '').split('/', 1)
                        tile = download(cos_client, bucket, key, tmpdir)
                        tiles.append(tile)
                    if bbox[2] > bbox[0]:
                        merged_image = os.path.join(download_dir, layer, 'merged.tif')
                        if force_bbox:
                            merge(tiles, merged_image, bbox)
                        else:
                            merge(tiles, merged_image)
                        merged_images.append(merged_image)
                    # If the given bbox spans over anti-meridian, we have to merge into two parts. Note that this will
                    # also overwrite the value of force_bbox and set it to true.
                    else:
                        merged_image_1 = os.path.join(download_dir, layer, 'merged_1.tif')
                        merged_image_2 = os.path.join(download_dir, layer, 'merged_2.tif')
                        merge(tiles, merged_image_1, (bbox[0], bbox[1], 180, bbox[3]))
                        merge(tiles, merged_image_2, (-180, bbox[1], bbox[2], bbox[3]))
                        merged_images.extend([merged_image_1, merged_image_2])
        return merged_images
    else:
        tiles = []
        for _, row in df.iterrows():
            layer = row['layer']
            date = row['date']
            url = row['url']
            bucket, key = url.replace('cos://', '').split('/', 1)
            tile = download(cos_client, bucket, key, os.path.join(download_dir, layer, str(date)))
            tiles.append(tile)
        return tiles
