# pyrip

## About

pyrip provides a series of convenient functions to process raster data. It is developed by IBM and liscenced under MIT License.

## Why build this?

This library is built on top of GDAL but aiming to provide a better alternative:

- easier-to-use. For new users in raster data processing domain, it can be often confusing and painful to use GDAL directly.
For example, you may find gdal_translate has tens of optional args as it supports all various of tasks including rescaling, 
format conversion, adding preview, etc. And it is so easy to mess up with the args which could result in unexpected result.
We build higher level apis on top of GDAL and provide specific args per each function to deliver easier-to-use apis.

- faster. For some operations, we re-engineered the implementations to make them faster. One example is 
converting raster images to pandas dataframe. Our new implementation is about 10x faster than the pure gdal
implementation. 

## Notes

1. Compatibility with gdal 3 is not yet tested and it is recommended to use gdal 2.3.3 at this stage. One of the easiest
way would be installing gdal 2.3.3 with conda main channel with `conda install gdal=2.3.3`
2. This library is mainly used in IBM Cloud for cloud native satellite data analysis. Thus some cloud functions are added 
and particularly tuned for IBM Cloud.
