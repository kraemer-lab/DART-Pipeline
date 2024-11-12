"""Plot data."""
from pathlib import Path
import logging
import re

from matplotlib import pyplot as plt
import geopandas as gpd
import numpy as np

from .util import output_path


def plot_heatmap(source, data, pdate, title, colourbar_label):
    """Create a heat map."""
    data[data == 0] = np.nan
    plt.imshow(data, cmap='coolwarm', origin='upper')
    plt.colorbar(label=colourbar_label)
    plt.title(title)
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', '_', title)
    title = title.strip()
    # Export
    path = Path(
        output_path(source), str(pdate).replace('-', '/'), title + '.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info('exporting:%s', path)
    plt.savefig(path)
    plt.close()


def plot_gadm_heatmap(
    source, data, gdf, pdate, title, colourbar_label, region, extent
):
    """Create a heat map with GADM region overlaid."""
    geometry = region.geometry
    min_lon, min_lat, max_lon, max_lat = geometry.bounds
    _, ax = plt.subplots()
    im = ax.imshow(data, cmap='coolwarm', origin='upper', extent=extent)
    # Add the geographical borders
    gdf.plot(ax=ax, color='none', edgecolor='gray')
    gpd.GeoDataFrame([region]).plot(ax=ax, color='none', edgecolor='k')
    # Add colour bar
    plt.colorbar(im, ax=ax, label=colourbar_label)
    # Titles and axes
    ax.set_title(title)
    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', '_', title)
    title = title.strip()
    # Export
    path = Path(
        output_path(source), str(pdate).replace('-', '/'), title + '.png'
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info('exporting:%s', path)
    plt.savefig(path)
    plt.close()


def plot_rwi_gadm_heatmap(source, rwi, shapefile, title):
    """Create a heat map of Relative Wealth Index with GADM region overlaid."""
    min_lon = rwi['longitude'].min()
    max_lon = rwi['longitude'].max()
    min_lat = rwi['latitude'].min()
    max_lat = rwi['latitude'].max()
    # Plot
    _, ax = plt.subplots()
    shapefile.boundary.plot(ax=ax, edgecolor='k', linewidth=0.5, zorder=0)
    extent = [min_lon, max_lon, min_lat, max_lat]
    data = rwi.pivot(columns='longitude', index='latitude', values='rwi')
    im = ax.imshow(data, cmap='coolwarm', origin='lower', extent=extent)
    # Add colour bar
    plt.colorbar(im, ax=ax, label='Relative Wealth Index [unitless]')
    # Titles and axes
    ax.set_title(title)
    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', '_', title)
    title = title.strip()
    # Export
    path = Path(output_path(source), title + '.png')
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info('exporting:%s', path)
    plt.savefig(path)
    plt.close()
