from setuptools import setup, find_packages

version = {}
exec(open('pyrip/version.py').read(), version)
VERSION = version['__version__']

setup(
    name='pyrip',
    version=VERSION,
    description='Raster Image Processor - a thin wrap that provides some convenient functions over gdal',
    packages=find_packages(),
    url='https://github.ibm.com/Common-Spatiotemporal-Analytics-Library/pyrip',
    author='Linsong Chu',
    author_email='lchu@us.ibm.com',
    install_requires=['rasterio', 'pandas', 'six', 'numpy']
)
