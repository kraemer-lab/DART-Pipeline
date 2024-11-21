"""Plot data."""
from pathlib import Path
import logging
import re

from matplotlib import pyplot as plt
import geopandas as gpd
import numpy as np

from .util import output_path


def plot_heatmap(data, title, colourbar_label, path):
    """Create a heat map."""
    data[data == 0] = np.nan
    plt.imshow(data, cmap='coolwarm', origin='upper')
    plt.colorbar(label=colourbar_label)
    plt.title(title)
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', '_', title)
    title = title.strip()
    # Export
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info('exporting:%s', path)
    plt.savefig(path)
    plt.close()


def plot_gadm_micro_heatmap(
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


def plot_gadm_macro_heatmap(
    data, origin, extent, limits, gdf, zorder, title, colourbar_label, path,
    log_plot=False
):
    """Create a heat map with a macro GADM region overlaid."""
    _, ax = plt.subplots()
    gdf.boundary.plot(ax=ax, edgecolor='k', linewidth=0.5, zorder=zorder)
    im = ax.imshow(data, cmap='coolwarm', origin=origin, extent=extent)
    # Add colour bar
    cbar = plt.colorbar(im, ax=ax, label=colourbar_label)
    # Raise the ticklabels to the power of e
    if log_plot:
        min_val, max_val = np.nanmin(data), np.nanmax(data)
        ticks = cbar.get_ticks()
        ticks = [t for t in ticks if (t > min_val) and (t < max_val)]
        cbar.set_ticks(ticks)
        cbar.set_ticklabels([f'{np.exp(tick):.2f}' for tick in ticks])
    # Titles and axes
    ax.set_title(title)
    ax.set_xlim(limits[0], limits[2])
    ax.set_ylim(limits[1], limits[3])
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', '_', title)
    title = title.strip()
    # Export
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info('exporting:%s', path)
    plt.savefig(path)
    plt.close()
