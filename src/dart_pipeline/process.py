"""
Functions to process raw data that has already been collated.

To process GADM administrative map geospatial data, run one or more of the
following commands

.. code-block::

    $ uv run dart-pipeline process geospatial/gadm admin-level=1


In general, use `EPSG:9217 <https://epsg.io/9217>`_ or
`EPSG:4326 <https://epsg.io/4326>`_ for map projections and use the
`ISO 3166-1 alpha-3 <https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3>`_
format for country codes.
"""
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal, Callable
import logging
import os
import re

from matplotlib import pyplot as plt
from pandarallel import pandarallel
import geopandas as gpd
import netCDF4 as nc
import numpy as np
import pandas as pd
import rasterio
import rasterio.features
import rasterio.mask
import rasterio.transform
import shapely.geometry

from .geospatial.aphroditeprecipitation import \
    process_gadm_aphroditeprecipitation
from .geospatial.aphroditetemperature import process_gadm_aphroditetemperature
from .geospatial.era5reanalysis import process_gadm_era5reanalysis
from .geospatial.worldpop_count import process_gadm_worldpopcount
from .geospatial.worldpop_density import process_gadm_worldpopdensity
from .meteorological.aphroditeprecipitation import \
    process_aphroditeprecipitation
from .meteorological.aphroditetemperature import process_aphroditetemperature
from .meteorological.era5reanalysis import process_era5reanalysis
from .population_weighted.relative_wealth_index import \
    process_gadm_popdensity_rwi
from .sociodemographic.worldpop_count import process_worldpopcount
from .sociodemographic.worldpop_density import process_worldpopdensity
from .constants import TERRACLIMATE_METRICS, OUTPUT_COLUMNS, BASE_DIR, \
    DEFAULT_SOURCES_ROOT, DEFAULT_OUTPUT_ROOT, MIN_FLOAT
from .plots import \
    plot_heatmap, plot_gadm_micro_heatmap, plot_gadm_macro_heatmap, \
    plot_timeseries
from .types import ProcessResult, PartialDate, AdminLevel
from .util import \
    source_path, output_path, get_country_name, get_shapefile

pandarallel.initialize(verbose=0)

TEST_MODE = os.getenv("DART_PIPELINE_TEST")
# No data in APHRODITE data
# See APHRO_MA_025deg_V1901.ctl and others
NO_DATA = -99.90


def process_rwi(iso3: str, admin_level: str, plots=False):
    """Process Relative Wealth Index data."""
    source = 'economic/relative-wealth-index'
    iso3 = iso3.upper()
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('plots:%s', plots)

    # Create a dictionary of polygons where the key is the ID of the polygon
    # and the value is its geometry
    path = get_shapefile(iso3, admin_level)
    logging.info('Importing:%s', path)
    gdf = gpd.read_file(path)
    admin_geoid = f'GID_{admin_level}'
    polygons = dict(zip(gdf[admin_geoid], gdf['geometry']))

    # Import the Relative Wealth Index data
    path = source_path(source, f'{iso3.lower()}_relative_wealth_index.csv')
    logging.info('Importing:%s', path)
    rwi = pd.read_csv(path)

    # Create a plot
    if plots:
        data = rwi.pivot(columns='longitude', index='latitude', values='rwi')
        origin = 'lower'
        min_lon = rwi['longitude'].min()
        max_lon = rwi['longitude'].max()
        min_lat = rwi['latitude'].min()
        max_lat = rwi['latitude'].max()
        extent = [min_lon, max_lon, min_lat, max_lat]
        limits = [min_lon, min_lat, max_lon, max_lat]
        zorder = 0
        country = get_country_name(iso3)
        title = f'Relative Wealth Index\n{country} - Admin Level {admin_level}'
        colourbar_label = 'Relative Wealth Index [unitless]'
        path = Path(output_path(source), f'{iso3}/admin_level_{admin_level}')
        plot_gadm_macro_heatmap(
            data, origin, extent, limits, gdf, zorder, title, colourbar_label,
            path
        )

    def get_admin(x):
        return get_admin_region(x['latitude'], x['longitude'], polygons)

    # Assign each latitude and longitude to an admin region
    rwi['geo_id'] = rwi.parallel_apply(get_admin, axis=1)  # type: ignore
    rwi = rwi[rwi['geo_id'] != 'null']

    # Get the mean RWI value for each region
    rwi = rwi.groupby('geo_id')['rwi'].mean().reset_index()

    # Dynamically choose which columns need to be added to the data
    region_columns = ['COUNTRY', 'NAME_1', 'NAME_2', 'NAME_3']
    admin_columns = region_columns[:int(admin_level) + 1]
    # Merge with the shapefile to get the region names
    rwi = rwi.merge(
        gdf[[admin_geoid] + admin_columns],
        left_on='geo_id', right_on=admin_geoid, how='left'
    )

    # Rename the columns
    columns = dict(zip(
        admin_columns, [f'admin_level_{i}' for i in range(len(admin_columns))]
    ))
    rwi = rwi.rename(columns=columns)
    rwi = rwi.rename(columns={'rwi': 'value'})
    # Add in the higher-level admin levels
    for i in range(int(admin_level) + 1, 4):
        rwi[f'admin_level_{i}'] = None
    # Add the metric name and unit
    rwi['metric'] = 'Relative Wealth Index'
    rwi['unit'] = 'unitless'
    # Re-order the columns
    output_columns = \
        [f'admin_level_{i}' for i in range(4)] + ['metric', 'value', 'unit']
    rwi = rwi[output_columns]

    return rwi, f'{iso3}.csv'


def process_dengueperu(
    admin_level: Literal['0', '1'] | None = None, plots=False
):
    """Process data from the Ministerio de Salud - Peru."""
    source = 'epidemiological/dengue/peru'
    if not admin_level:
        admin_level = '0'
        logging.info('admin_level:None (defaulting to %s)', admin_level)
    elif admin_level in ['0', '1']:
        logging.info('admin_level:%s', admin_level)
    else:
        raise ValueError(f'Invalid admin level: {admin_level}')

    # Find the raw data
    path = source_path(source)
    if admin_level == '0':
        filepaths = [Path(path, 'casos_dengue_nacional.xlsx')]
    else:
        filepaths = []
        for dirpath, _, filenames in os.walk(path):
            filenames.sort()
            for filename in filenames:
                # Skip hidden files
                if filename.startswith('.'):
                    continue
                # Skip admin levels that have not been requested for analysis
                if filename == 'casos_dengue_nacional.xlsx':
                    continue
                filepaths.append(Path(dirpath, filename))

    # Initialise an output data frame
    master = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Import the raw data
    for filepath in filepaths:
        logging.info('importing:%s', filepath)
        df = pd.read_excel(filepath)

        # Rename the headings
        columns = {
            'ano': 'year',
            'semana': 'week',
            'tipo_dx': 'metric',
            'n': 'value'
        }
        df = df.rename(columns=columns)

        # Define two metrics
        df.loc[df['metric'] == 'C', 'metric'] = 'Confirmed Dengue Cases'
        df.loc[df['metric'] == 'P', 'metric'] = 'Probable Dengue Cases'
        # Confirm no rows have been missed
        metrics = ['Confirmed Dengue Cases', 'Probable Dengue Cases']
        mask = ~df['metric'].isin(metrics)
        assert len(df[mask]) == 0

        # Get the name of the administrative divisions
        filename = filepath.name
        name = filename.removesuffix('.xlsx').split('_')[-1].capitalize()
        logging.info('processing:%s', name)
        # Add to the output data frame
        df['admin_level_0'] = 'Peru'
        if admin_level == '1':
            df['admin_level_1'] = name
        else:
            df['admin_level_1'] = ''
        df['admin_level_2'] = ''
        df['admin_level_3'] = ''

        # Add to master data frame
        master = pd.concat([master, df], ignore_index=True)

        # Plot
        if plots:
            df['date'] = pd.to_datetime(
                df['year'].astype(str) + df['week'].astype(str) + '1',
                format='%Y%U%w'
            )
            start = df.loc[0, 'year']
            end = df.loc[len(df) - 1, 'year']
            if admin_level == '0':
                title = f'Dengue Cases\nPeru - {start} to {end}'
            else:
                title = f'Dengue Cases\n{name} - {start} to {end}'
            path = Path(output_path(source), name + '.png')
            plot_timeseries(df, title, path)

    # Fill in additional columns
    master['iso3'] = 'PER'
    master['month'] = ''
    master['day'] = ''
    master['unit'] = 'cases'
    master['resolution'] = f'admin{admin_level}'
    master['creation_date'] = date.today()

    return master, 'dengue_peru.csv'


def process_gadm_chirps_rainfall(
    iso3: str, admin_level: Literal['0', '1'], partial_date: str, plots=False
):
    """
    Process GADM administrative map and CHIRPS rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.
    """
    pdate = PartialDate.from_string(partial_date)
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('partial_date:%s', pdate)
    logging.info('scope:%s', pdate.scope)
    logging.info('plots:%s', plots)

    # Import the GeoTIFF file
    file = get_chirps_rainfall_data_path(pdate)
    logging.info('importing:%s', file)
    src = rasterio.open(file)

    # Create a bounding box from raster bounds
    bounds = src.bounds
    raster_bbox = shapely.geometry.box(
        bounds.left, bounds.bottom, bounds.right, bounds.top
    )

    # Import shape file
    path = get_shapefile(iso3, admin_level)
    logging.info('importing:%s', path)
    gdf = gpd.read_file(path)
    # Transform the shape file to match the GeoTIFF's coordinate system
    gdf = gdf.to_crs(src.crs)
    # EPSG:4326 - WGS 84: latitude/longitude coordinate system based on the
    # Earth's center of mass

    # Initialise the data frame that will store the output data for each region
    columns = [
        'admin_level_0', 'admin_level_1', 'admin_level_2', 'admin_level_3',
        'year', 'month', 'day', 'rainfall'
    ]
    output = pd.DataFrame(columns=columns)

    # Iterate over each region in the shape file
    for i, region in gdf.iterrows():
        # Add the region name to the output data frame
        output.loc[i, 'admin_level_0'] = region['COUNTRY']
        # Initialise the graph title
        title = region['COUNTRY']
        # Add more region names and update the graph title if the admin level
        # is high enough to warrant it
        if int(admin_level) >= 1:
            output.loc[i, 'admin_level_1'] = region['NAME_1']
            title = region['NAME_1']
        if int(admin_level) >= 2:
            output.loc[i, 'admin_level_2'] = region['NAME_2']
            title = region['NAME_2']
        if int(admin_level) >= 3:
            output.loc[i, 'admin_level_3'] = region['NAME_3']
            title = region['NAME_3']

        # Add date information to the output data frame
        output.loc[i, 'year'] = pdate.year
        if pdate.month:
            output.loc[i, 'month'] = pdate.month
        if pdate.day:
            output.loc[i, 'day'] = pdate.day

        # Check if the rainfall data intersects this region
        geometry = region.geometry
        if raster_bbox.intersects(geometry):
            # There is rainfall data for this region
            # Clip the data using the polygon of the current region. By
            # default, a pixel is included only if its center is within one of
            # the shapes
            region_data, _ = rasterio.mask.mask(src, [geometry], crop=True)
            # Replace negative values (if any exist)
            region_data = np.where(region_data < 0, np.nan, region_data)
            # Sum the pixel values to get the total for the region
            region_total = np.nansum(region_data)
        else:
            # No rainfall data for this region
            region_total = 0
        logging.info('region:%s', title)
        logging.info('region_total:%s', region_total)
        # Add the result to the output data frame
        output.loc[i, 'rainfall'] = region_total

        if plots:
            # Get the bounds of the region
            min_lon, min_lat, max_lon, max_lat = geometry.bounds
            # Plot
            _, ax = plt.subplots()
            ar = region_data[0]
            ar[ar == 0] = np.nan
            im = ax.imshow(
                ar, cmap='coolwarm', origin='upper',
                extent=[min_lon, max_lon, min_lat, max_lat]
            )
            # Add the geographical borders
            gdf.plot(ax=ax, color='none', edgecolor='gray')
            gpd.GeoDataFrame([region]).plot(ax=ax, color='none', edgecolor='k')
            plt.colorbar(im, ax=ax, label='Rainfall [mm]')
            ax.set_title(f'Rainfall\n{title} - {pdate}')
            ax.set_xlim(min_lon, max_lon)
            ax.set_ylim(min_lat, max_lat)
            ax.set_ylabel('Latitude')
            ax.set_xlabel('Longitude')
            # Make the plot title file-system safe
            title = re.sub(r'[<>:"/\\|?*]', '_', title)
            title = title.strip()
            # Export
            path = Path(
                output_path('geospatial/chirps-rainfall'),
                str(pdate).replace('-', '/'), title + '.png'
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            logging.info('exporting:%s', path)
            plt.savefig(path)
            plt.close()

    # Export
    return output, f'{iso3}.csv'


def process_gadm_admin_map_data(iso3: str, admin_level: AdminLevel):
    """Process GADM administrative map data."""
    gdf = gpd.read_file(get_shapefile(iso3, admin_level))

    # en.wikipedia.org/wiki/List_of_national_coordinate_reference_systems
    national_crs = {
        "GBR": "EPSG:27700",
        "PER": "EPSG:24892",  # Peru central zone
        "VNM": "EPSG:4756",
    }
    try:
        gdf = gdf.to_crs(national_crs[iso3])
    except KeyError:
        pass

    # Initialise output data frame
    output = pd.DataFrame()
    # Iterate over the regions in the shape file
    for _, region in gdf.iterrows():
        # Initialise a new row for the output data frame
        new_row = {"Admin Level 0": region["COUNTRY"]}
        # Initialise the title
        # Update the new row and the title if the admin level is high enough
        if int(admin_level) >= 1:
            new_row["Admin Level 1"] = region["NAME_1"]
        if int(admin_level) >= 2:
            new_row["Admin Level 2"] = region["NAME_2"]
        if int(admin_level) >= 3:
            new_row["Admin Level 3"] = region["NAME_3"]

        # Calculate area in square metres
        area = region.geometry.area
        # Convert to square kilometres
        area_sq_km = area / 1e6
        # Add to output data frame
        new_row["Area [km²]"] = area_sq_km
        new_row_df = pd.DataFrame(new_row, index=[0])
        output = pd.concat([output, new_row_df], ignore_index=True)

    return output, f"{iso3}/admin{admin_level}_area.csv"


def get_chirps_rainfall_data_path(date: PartialDate) -> Path:
    """Get the path to a CHIRPS rainfall data file."""
    file = None
    match date.scope:
        case "daily":
            file = Path(
                "global_daily",
                str(date.year),
                date.zero_padded_month,
                f"chirps-v2.0.{date.to_string('.')}.tif",
            )
        case "monthly":
            file = Path(
                "global_monthly",
                str(date.year),
                f"chirps-v2.0.{date.to_string('.')}.tif",
            )
        case "annual":
            file = Path("global_annual", f"chirps-v2.0.{date}.tif")

    path = source_path("meteorological/chirps-rainfall", file)
    if not path.exists():
        raise FileNotFoundError(f"CHIRPS rainfall data not found: {path}")

    return path


def process_chirps_rainfall(partial_date: str, plots=False) -> ProcessResult:
    """
    Process CHIRPS Rainfall data.

    "CHIRPS" stands for Climate Hazards Group InfraRed Precipitation with
    Station.
    """
    source = 'meteorological/chirps-rainfall'
    pdate = PartialDate.from_string(partial_date)
    logging.info('partial_date:%s', pdate)
    logging.info('scope:%s', pdate.scope)
    logging.info('plots:%s', plots)

    # Import the GeoTIFF file
    file = get_chirps_rainfall_data_path(pdate)
    logging.info('importing:%s', file)
    src = rasterio.open(file)

    # Initialise the data frame that will store the output data for each region
    columns = ['year', 'month', 'day', 'rainfall']
    output = pd.DataFrame(columns=columns)

    # Add date information to the output data frame
    output.loc[0, 'year'] = pdate.year
    if pdate.month:
        output.loc[0, 'month'] = pdate.month
    if pdate.day:
        output.loc[0, 'day'] = pdate.day

    # Rasterio stores image layers in 'bands'
    # Get the data in the first band as an array
    data = src.read(1)
    # Replace placeholder numbers with 0
    data[data == MIN_FLOAT] = 0
    # Hide nulls
    data[data == -9999] = 0
    # Add the result to the output data frame
    output.loc[0, 'rainfall'] = np.nansum(data)

    # Create a plot
    if plots:
        title = f'Rainfall\n{pdate}'
        colourbar_label = 'Rainfall [mm]'
        path = Path(
            output_path(source), str(pdate).replace('-', '/'),
            str(pdate) + '.png'
        )
        plot_heatmap(data, title, colourbar_label, path)

    # Export
    return output, 'chirps-rainfall.csv'


def process_terraclimate(
    partial_date: str, iso3: str, admin_level: str, plots=False
):
    """
    Process TerraClimate data.

    This metric incorporates TerraClimate gridded temperature, precipitation,
    and other water balance variables. The data is stored in NetCDF (`.nc`)
    files for which the `netCDF4` library is needed.
    """
    source = 'meteorological/terraclimate'
    pdate = PartialDate.from_string(partial_date)
    logging.info('partial_date:%s', pdate)
    iso3 = iso3.upper()
    logging.info('iso3:%s', iso3)
    logging.info('admin_level:%s', admin_level)
    logging.info('plots:%s', plots)

    # Initialise output data frame
    columns = [
        'admin_level_0', 'admin_level_1', 'admin_level_2', 'admin_level_3',
        'year', 'month'
    ]
    output = pd.DataFrame(columns=columns)

    # Import a shapefile
    path = BASE_DIR / DEFAULT_SOURCES_ROOT / 'geospatial' / 'gadm' / iso3 / \
        f'gadm41_{iso3}_{admin_level}.shp'
    logging.info('importing:%s', path)
    gdf = gpd.read_file(path)

    # Iterate over the metrics
    for metric in TERRACLIMATE_METRICS:
        # Import the raw data
        if (pdate.year == 2023) and (metric == 'pdsi'):
            # In 2023 the capitalization of pdsi changed
            filename = f'TerraClimate_PDSI_{pdate.year}.nc'
            metric = 'PDSI'
        else:
            filename = f'TerraClimate_{metric}_{pdate.year}.nc'
        path = BASE_DIR / DEFAULT_SOURCES_ROOT / source / filename
        logging.info('importing:%s', path)
        ds = nc.Dataset(path)

        # Extract the variables
        lat = ds.variables['lat'][:]
        lon = ds.variables['lon'][:]
        time = ds.variables['time'][:]  # Time in days since 1900-01-01
        raw = ds.variables[metric]

        # Check if a standard name is provided for this metric
        try:
            standard_name = raw.standard_name
        except AttributeError:
            standard_name = metric

        # Apply scale factor
        data = raw[:].astype(np.float32)
        data = data * raw.scale_factor + raw.add_offset
        # Replace fill values with NaN
        data[data == raw._FillValue] = np.nan

        # Convert time to actual dates
        base_time = datetime(1900, 1, 1)
        months = [base_time + timedelta(days=t) for t in time]

        for i, month in enumerate(months):
            # If a month has been specified on the command line
            if pdate.month:
                # If this data come from a month that does not match the
                # requested month
                if pdate.month != month.month:
                    # Skip this iteration
                    continue

            # Extract the data for the chosen month
            this_month = data[i, :, :]

            # Plot
            if plots:
                origin = 'upper'
                extent = [lon.min(), lon.max(), lat.min(), lat.max()]
                limits = gdf.total_bounds
                zorder = 1
                month_str = month.strftime('%B %Y')
                title = f'{raw.description}\n{iso3} - {month_str}'
                colourbar_label = f'{raw.description} [{raw.units}]'
                path = BASE_DIR / DEFAULT_OUTPUT_ROOT / source / \
                    str(pdate).replace('-', '/') / \
                    f'admin_level_{admin_level}' / \
                    (title.replace('\n', ' - ') + '.png')
                plot_gadm_macro_heatmap(
                    this_month, origin, extent, limits, gdf, zorder, title,
                    colourbar_label, path
                )

            # Iterate over the regions in the shape file
            for j, region in gdf.iterrows():
                geometry = region.geometry

                # Initialise a new row for the output data frame
                idx = i * len(months) + j
                output.loc[idx, 'admin_level_0'] = region['COUNTRY']
                output.loc[idx, 'admin_level_1'] = None
                output.loc[idx, 'admin_level_2'] = None
                output.loc[idx, 'admin_level_3'] = None
                output.loc[idx, 'year'] = month.year
                output.loc[idx, 'month'] = month.month
                # Initialise the graph title
                region_name = region['COUNTRY']
                # Update the new row and the title if the admin level is high
                # enough
                if int(admin_level) >= 1:
                    output.loc[idx, 'admin_level_1'] = region['NAME_1']
                    region_name = region['NAME_1']
                if int(admin_level) >= 2:
                    output.loc[idx, 'admin_level_2'] = region['NAME_2']
                    region_name = region['NAME_2']
                if int(admin_level) >= 3:
                    output.loc[idx, 'admin_level_3'] = region['NAME_3']
                    region_name = region['NAME_3']

                # Define transform for geometry_mask based on grid resolution
                transform = rasterio.transform.from_origin(
                    lon.min(), lat.max(), abs(lon[1] - lon[0]),
                    abs(lat[1] - lat[0])
                )

                # Create a mask that is True for points outside the geometries
                mask = rasterio.features.geometry_mask(
                    [geometry], transform=transform, out_shape=this_month.shape
                )
                masked_data = np.ma.masked_array(this_month, mask=mask)

                # Plot
                if plots and (admin_level == '0'):
                    month_str = month.strftime('%B %Y')
                    title = f'{raw.description}\n{region_name} - {month_str}'
                    colourbar_label = f'{raw.description} [{raw.units}]'
                    extent = [lon.min(), lon.max(), lat.min(), lat.max()]
                    path = Path(
                        output_path(source),
                        str(pdate).replace('-', '/'),
                        f'{region_name} - {metric}.png'
                    )
                    plot_gadm_micro_heatmap(
                        masked_data, gdf, pdate, title, colourbar_label,
                        region, extent, path
                    )

                # Add to output data frame
                output.loc[idx, standard_name] = np.nansum(masked_data)

        # Close the NetCDF file after use
        ds.close()

    return output, 'terraclimate.csv'


def get_admin_region(lat: float, lon: float, polygons) -> str:
    """
    Find the admin region in which a grid cell lies.

    Return the ID of administrative region in which the centre (given by
    latitude and longitude) of a 2.4km^2 grid cell lies.
    """
    point = shapely.geometry.Point(lon, lat)
    for geo_id in polygons:
        polygon = polygons[geo_id]
        if polygon.contains(point):
            return geo_id
    return "null"


PROCESSORS: dict[str, Callable[..., ProcessResult | list[ProcessResult]]] = {
    'economic/relative-wealth-index': process_rwi,
    'epidemiological/dengue/peru': process_dengueperu,
    'geospatial/aphrodite-daily-mean-temp': process_gadm_aphroditetemperature,
    'geospatial/aphrodite-daily-precip': process_gadm_aphroditeprecipitation,
    'geospatial/chirps-rainfall': process_gadm_chirps_rainfall,
    'geospatial/era5-reanalysis': process_gadm_era5reanalysis,
    'geospatial/gadm': process_gadm_admin_map_data,
    'geospatial/worldpop-count': process_gadm_worldpopcount,
    'geospatial/worldpop-density': process_gadm_worldpopdensity,
    'meteorological/aphrodite-daily-mean-temp': process_aphroditetemperature,
    'meteorological/aphrodite-daily-precip': process_aphroditeprecipitation,
    'meteorological/chirps-rainfall': process_chirps_rainfall,
    'meteorological/era5-reanalysis': process_era5reanalysis,
    'meteorological/terraclimate': process_terraclimate,
    'population-weighted/relative-wealth-index': process_gadm_popdensity_rwi,
    'sociodemographic/worldpop-count': process_worldpopcount,
    'sociodemographic/worldpop-density': process_worldpopdensity,
}
