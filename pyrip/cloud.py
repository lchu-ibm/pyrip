import os
import six
import tempfile


def download(cos_client, bucket, key, download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    cos_client.download_file(bucket, key, os.path.join(download_dir, os.path.basename(key)))
    return os.path.join(download_dir, os.path.basename(key))


def query_tiles_with_spark(spark, api_key, instance_crn, endpoint_url, bucket, image_meta_prefix, layers, start_date, end_date, bbox, download_dir, merge_tiles=True, force_bbox=True):
    if isinstance(layers, six.string_types):
        layers = [layers]

    # Get COS client for downloading tiles
    import ibm_boto3
    from ibm_botocore.client import Config
    cos_client = ibm_boto3.client("s3",
        ibm_api_key_id=api_key,
        ibm_service_instance_id=instance_crn,
        endpoint_url=endpoint_url,
        config=Config(signature_version="oauth")
    )

    # Config Spark for reading meta data
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set('fs.cos.myCos.endpoint', endpoint_url)
    sc._jsc.hadoopConfiguration().set('fs.cos.myCos.iam.api.key', api_key)
    sc._jsc.hadoopConfiguration().set('fs.cos.myCos.iam.service.id', instance_crn)

    # Get meta data
    spark.read.parquet('cos://{}.myCos/{}'.format(bucket, image_meta_prefix)).createOrReplaceTempView('temp_view')
    df = spark.sql("""
    SELECT *
    FROM temp_view
    WHERE layer in {0} AND date between {1} AND {2}
    AND lat between {3[1]} AND {3[3]} AND lon between {3[0]} AND {3[2]}
    """.format(tuple(layers), start_date, end_date, bbox)).toPandas()

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    if merge_tiles:
        from pyrip.image import merge
        merged_images = []
        for layer in layers:
            tmp_df = df[df['layer'] == layer].sort_values(['date', 'lat', 'lon'])
            tiles = []
            with tempfile.TemporaryDirectory() as tmpdir:
                for url in tmp_df['url']:
                    bucket, key = url.replace('cos://', '').split('/', 1)
                    tile = download(cos_client, bucket, key, tmpdir)
                    tiles.append(tile)
                merged_image = os.path.join(download_dir, layer, 'merged.tif')
                if force_bbox:
                    merge(tiles, merged_image, bbox)
                else:
                    merge(tiles, merged_image)
                merged_images.append(merged_image)
        return merged_images
    else:
        tiles = []
        for layer in layers:
            tmp_df = df[df['layer'] == layer].sort_values(['date', 'lat', 'lon'])
            for url in tmp_df['url']:
                bucket, key = url.replace('cos://', '').split('/', 1)
                tile = download(cos_client, bucket, key, os.path.join(download_dir, layer))
                tiles.append(tile)
        return tiles


def query_tiles(sql_client, cos_client, cos_url, target_cos_url, layers, start_date, end_date, bbox, download_dir, merge_tiles=True, force_bbox=True):
    if isinstance(layers, six.string_types):
        layers = [layers]

    # By default, the result will be written into target_cos in CSV format. We disable this and make it write as
    # parquet to improvement performance
    if sql_client.target_cos is not None:
        sql_client.target_cos = None

    query_str = """
    SELECT *
    FROM {0} STORED AS PARQUET
    WHERE layer in {1} AND date between {2} AND {3}
    AND lat between {4[1]} AND {4[3]} AND lon between {4[0]} AND {4[2]}
    INTO {5} STORED AS parquet
    """.format(cos_url, tuple(layers), start_date, end_date, bbox, target_cos_url)
    # ST_Contains(ST_WKTToSQL('BOUNDINGBOX({} {}, {} {})'), ST_Point(lon, lat))

    df = sql_client.run_sql(query_str)

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    if merge_tiles:
        from pyrip.image import merge
        merged_images = []
        for layer in layers:
            tmp_df = df[df['layer'] == layer].sort_values(['date', 'lat', 'lon'])
            tiles = []
            with tempfile.TemporaryDirectory() as tmpdir:
                for url in tmp_df['url']:
                    bucket, key = url.replace('cos://', '').split('/', 1)
                    tile = download(cos_client, bucket, key, tmpdir)
                    tiles.append(tile)
                merged_image = os.path.join(download_dir, layer, 'merged.tif')
                if force_bbox:
                    merge(tiles, merged_image, bbox)
                else:
                    merge(tiles, merged_image)
                merged_images.append(merged_image)
        return merged_images
    else:
        tiles = []
        for layer in layers:
            tmp_df = df[df['layer'] == layer].sort_values(['date', 'lat', 'lon'])
            for url in tmp_df['url']:
                bucket, key = url.replace('cos://', '').split('/', 1)
                tile = download(cos_client, bucket, key, os.path.join(download_dir, layer))
                tiles.append(tile)
        return tiles
