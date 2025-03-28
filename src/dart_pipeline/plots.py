"""Plot data."""

from datetime import date
import logging
import re

from matplotlib import pyplot as plt
import geopandas as gpd
import numpy as np


def plot_heatmap(data, title, colourbar_label, path, extent=None, log_plot=False):
    """Create a heat map."""
    data[data == 0] = np.nan
    _, ax = plt.subplots()
    im = ax.imshow(data, cmap="coolwarm", origin="upper", extent=extent)
    # Add colour bar
    cbar = plt.colorbar(im, ax=ax, label=colourbar_label)
    # Raise the ticklabels to the power of e
    if log_plot:
        min_val, max_val = np.nanmin(data), np.nanmax(data)
        ticks = cbar.get_ticks()
        ticks = [t for t in ticks if (t > min_val) and (t < max_val)]
        cbar.set_ticks(ticks)
        cbar.set_ticklabels([f"{np.exp(tick):.0f}" for tick in ticks])
    # Titles and axes
    ax.set_title(title)
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', "_", title)
    title = title.strip()
    # Export
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("exporting:%s", path)
    plt.savefig(path)
    plt.close()


def plot_gadm_micro_heatmap(
    data, gdf, pdate, title, colourbar_label, region, extent, path
):
    """Create a heat map with GADM region overlaid."""
    geometry = region.geometry
    min_lon, min_lat, max_lon, max_lat = geometry.bounds
    _, ax = plt.subplots()
    im = ax.imshow(data, cmap="coolwarm", origin="upper", extent=extent)
    # Add the geographical borders
    gdf.plot(ax=ax, color="none", edgecolor="gray")
    gpd.GeoDataFrame([region]).plot(ax=ax, color="none", edgecolor="k")
    # Add colour bar
    plt.colorbar(im, ax=ax, label=colourbar_label)
    # Titles and axes
    ax.set_title(title)
    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', "_", title)
    title = title.strip()
    # Export
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("exporting:%s", path)
    plt.savefig(path)
    plt.close()


def plot_gadm_macro_heatmap(
    data,
    origin,
    extent,
    limits,
    gdf,
    zorder,
    title,
    colourbar_label,
    path,
    log_plot=False,
):
    """Create a heat map with a macro GADM region overlaid."""
    _, ax = plt.subplots()
    gdf.boundary.plot(ax=ax, edgecolor="k", linewidth=0.5, zorder=zorder)
    im = ax.imshow(data, cmap="coolwarm", origin=origin, extent=extent)
    # Add colour bar
    cbar = plt.colorbar(im, ax=ax, label=colourbar_label)
    # Raise the ticklabels to the power of e
    if log_plot:
        min_val, max_val = np.nanmin(data), np.nanmax(data)
        ticks = cbar.get_ticks()
        ticks = [t for t in ticks if (t > min_val) and (t < max_val)]
        cbar.set_ticks(ticks)
        cbar.set_ticklabels([f"{np.exp(tick):.2f}" for tick in ticks])
    # Titles and axes
    ax.set_title(title)
    ax.set_xlim(limits[0], limits[2])
    ax.set_ylim(limits[1], limits[3])
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    # Make the plot title file-system safe
    title = re.sub(r'[<>:"/\\|?*]', "_", title)
    title = title.strip()
    # Export
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("exporting:%s", path)
    plt.savefig(path)
    plt.close()


def plot_timeseries(df, title, path):
    """Plot time series data."""
    plt.figure()
    for metric in df["metric"].unique():
        subset = df[df["metric"] == metric]
        plt.plot(subset["date"], subset["value"], label=metric)
    for year in range(df["year"].min(), df["year"].max() + 1):
        year_dt = date(year, 1, 1)
        plt.axvline(year_dt, linestyle="--", alpha=0.3, c="gray")
    plt.title(title)
    plt.xlabel("Year", fontsize=12)
    plt.ylabel("Cases", fontsize=12)
    plt.xticks(rotation=30)
    ymin, ymax = plt.ylim()
    plt.ylim(0, ymax)
    plt.xlim(date(df["year"].min(), 1, 1), date(df["year"].max(), 12, 31))
    plt.legend()
    plt.tight_layout()
    # Export
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("exporting:%s", path)
    plt.savefig(path)
    plt.close()


def plot_scatter(x, y, z, title, colourbar_label, path):
    """Plot a scatter plot."""
    plt.figure()
    scatter = plt.scatter(x, y, c=z, cmap="coolwarm", s=10)
    plt.colorbar(scatter, label=colourbar_label)
    plt.title(title)
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(True)
    # Export
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path)
    plt.close()


def plot_gadm_scatter(lon, lat, data, title, colourbar_label, path, gdf):
    """Plot a scatter plot."""
    fig, ax = plt.subplots()
    scatter = ax.scatter(lon, lat, c=data, cmap="coolwarm", marker="o", s=10)
    _ = fig.colorbar(scatter, ax=ax, label=colourbar_label)
    gdf.boundary.plot(ax=ax, color="black", linewidth=0.5)
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    # Set the axis limits to the bounding box of the shapefile (not the data)
    minx, miny, maxx, maxy = gdf.total_bounds
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)
    # Export
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path)
    plt.close()
