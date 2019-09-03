import os
import six
import tempfile


def download(cos_client, bucket, key, download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    cos_client.download_file(bucket, key, os.path.join(download_dir, os.path.basename(key)))
    return os.path.join(download_dir, os.path.basename(key))


def query_tiles(cos_client, cos_url, target_cos_url, layers, start_date, end_date, bbox, download_dir, sql_client=None, spark=None, merge_tiles=True, force_bbox=True):
    if isinstance(layers, six.string_types):
        layers = [layers]

    query_str = """
        SELECT *
        FROM {0} STORED AS PARQUET
        WHERE layer in {1} AND date between {2} AND {3}
        AND lat between {4[1]} AND {4[3]} AND lon between {4[0]} AND {4[2]}
        INTO {5} STORED AS parquet
        """.format(cos_url, tuple(layers), start_date, end_date, bbox, target_cos_url)
    # ST_Contains(ST_WKTToSQL('BOUNDINGBOX({} {}, {} {})'), ST_Point(lon, lat))

    if sql_client is not None:
        # By default, the result will be written into target_cos in CSV format. We disable this and make it write as
        # parquet to improvement performance
        if sql_client.target_cos is not None:
            sql_client.target_cos = None
        df = sql_client.run_sql(query_str)
    elif spark is not None:
        df = spark.sql(query_str).toPandas()
    else:
        raise ValueError('Either sql_client or spark has to be set.')

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
