import rasterio
from rasterio.plot import show


def plot(file):
    with rasterio.open(file) as dataset:
        show(dataset)
