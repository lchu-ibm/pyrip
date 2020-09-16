import rasterio
from rasterio.plot import show

# https://rasterio.readthedocs.io/en/latest/api/rasterio.plot.html?highlight=show#rasterio.plot.show
# https://matplotlib.org/api/_as_gen/matplotlib.axes.Axes.imshow.html#matplotlib-axes-axes-imshow


def plot(file, **kwargs):
    with rasterio.open(file) as dataset:
        show(dataset, **kwargs)
