# pyrip

## About
pyrip provides a series of convenient functions to process raster data.

## Why build this?

The main reason for building this thin wrapper library is to provide a more specific library for raster data processing
on satellite imagery within IBM Cloud. Note that this is not served as a general raster data processing library.

Another reason is that we want to improve some functionalities which has bad performance in gdal implementations. One
example is converting raster images to pandas dataframe. Our implementation is about 10x faster than the pure gdal
implementation. 

