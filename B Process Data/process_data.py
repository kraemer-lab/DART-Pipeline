"""
Process data.

Pre-requisites:

.. code-block::

    $ python3.12 -m pip install matplotlib
    $ python3.12 -m pip install shapely
    $ python3.12 -m pip install geopandas
    $ python3.12 -m pip install rasterio
    $ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    $ brew --version
    $ brew install gdal
    $ ogr2ogr --version

Use `EPSG:9217 <https://epsg.io/9217>`_

"""
from pathlib import Path
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as mticker
import os
import numpy as np
import json
from shapely.geometry import Point, Polygon
from shapely.affinity import scale
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask


def plot_pop_density(df, folderpath, filename):
    """Plot the population for a region."""
    # Plot
    A = 3  # We want figures to be A3
    figsize = (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
    fig = plt.figure(figsize=figsize, dpi=144)
    ax = plt.axes()
    # Re-scale
    df['Z_rescaled'] = df['Z']**0.01
    # Plot
    pivotted = df.pivot(index='Y', columns='X', values='Z_rescaled')
    cax = plt.imshow(pivotted, cmap='GnBu')
    # Manually create the colour bar
    ticks = np.linspace(df['Z_rescaled'].min(), df['Z_rescaled'].max(), 5)
    ticklabels = ticks**(1 / 0.01)
    ticklabels = ticklabels.astype(int)
    fig.colorbar(
        cax,
        ticks=ticks,
        format=mticker.FixedFormatter(ticklabels),
        shrink=0.2,
        label='Population Density 2020, UN Adjusted (pop/km²)'
    )
    # Turn upside down
    plt.gca().invert_yaxis()
    # Remove ticks and tick labels
    plt.axis('off')
    # Correct aspect ratio
    ax.set_aspect('equal', adjustable='datalim')
    ax.autoscale()
    # Save
    path = Path(folderpath, filename)
    plt.savefig(path)
    plt.close()


#
# Geospatial data
#

"""
GADM administrative map
"""
if False:
    filenames = [
        'gadm41_VNM_0.shp', 'gadm41_VNM_1.shp', 'gadm41_VNM_2.shp',
        'gadm41_VNM_3.shp'
    ]
    for filename in filenames:
        # Import the shape file
        filename = Path(filename)
        branch_path = Path(
            'Geospatial data', 'GADM administrative map', 'gadm41_VNM_shp',
        )
        path = Path('..', 'A Collate Data', branch_path, filename)
        gdf = gpd.read_file(path)
        # Plot
        fig = plt.figure(figsize=(10, 10))
        ax = plt.axes()
        gdf.plot(ax=ax)
        plt.title(filename)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        # Export
        os.makedirs(branch_path, exist_ok=True)
        path = Path(branch_path, filename.stem)
        plt.savefig(path)

#
# Socio-demographic data
#

"""
WorldPop population density
"""
# Label the location of each coordinate
# (takes approx 685.0s - 11:25 min - to download)
out_dir = Path('Socio-Demographic Data', 'WorldPop population density')
if False:
    # Create the output folder
    os.makedirs(out_dir, exist_ok=True)

    # Import the population density data for Vietnam
    path = Path(
        '..', 'A Collate Data', 'Socio-Demographic Data',
        'WorldPop population density', 'Population Density',
        'Unconstrained individual countries UN adjusted (1km resolution)',
        'Vietnam', 'vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip'
    )
    df = pd.read_csv(path)

    # Import the coordinates of the borders of Vietnam's regions
    path = Path('..', 'A Collate Data', 'Archive', 'VNM_ADM_2.geojson')
    with open(path) as file:
        geojson = json.load(file)

    # Classify the location of each coordinate
    df['Country'] = None
    df['Admin 2'] = None
    df['Admin 3'] = None
    # Pre-construct the polygons
    polygons = []
    regions = []
    for feature in geojson['features']:
        polygon = Polygon(feature['geometry']['coordinates'][0][0])
        # print(polygon)
        polygons.append(polygon)
        region = (
            feature['properties']['COUNTRY'],
            feature['properties']['NAME_1'],
            feature['properties']['NAME_2']
        )
        regions.append(region)
    # Iterate over the coordinates
    for i, row in df.iterrows():
        # Update the user
        if i % 100 == 0:
            print(f'{i} / {len(df)}')
        point = Point(df.loc[i, 'X'], df.loc[i, 'Y'])
        for j, polygon in enumerate(polygons):
            # Check if the coordinate is in this region
            if point.within(polygon):
                # We have found the region this coordinate is in
                df.loc[i, 'Country'] = regions[j][0]
                df.loc[i, 'Admin 2'] = regions[j][1]
                df.loc[i, 'Admin 3'] = regions[j][2]
                # Move to the next line of the data frame
                break
    # Export
    path = Path(out_dir, 'Vietnam.csv')
    df.to_csv(path, index=False)

if False:
    # Import the labelled data
    path = Path(out_dir, 'Vietnam.csv')
    df = pd.read_csv(path)

    # Plot whole country
    plot_pop_density(df, out_dir, 'Vietnam.png')

    # Initialise output dictionaries
    dct_admin_2 = {}
    dct_admin_3 = {}

    # Create the output folders
    path = Path(out_dir, 'Admin 2')
    os.makedirs(path, exist_ok=True)

    # Analyse each province/city
    for admin_2 in df['Admin 2'].unique():
        if admin_2 is not np.nan:
            subset = df[df['Admin 2'] == admin_2].copy()
            print(admin_2)
            plot_pop_density(subset, path, f'{admin_2}.png')
            dct_admin_2[admin_2] = subset['Z'].mean()
            for admin_3 in subset['Admin 3'].unique():
                if admin_3 is not np.nan:
                    ssubset = subset[subset['Admin 3'] == admin_3].copy()
                    dct_admin_3[admin_3] = ssubset['Z'].mean()
    # Export
    path = Path(out_dir, 'WorldPop population density - Admin 2.json')
    with open(path, 'w') as file:
        json.dump(dct_admin_2, file)
    path = Path(out_dir, 'WorldPop population density - Admin 3.json')
    with open(path, 'w') as file:
        json.dump(dct_admin_3, file)

"""
WorldPop population count

- EPSG:9217: https://epsg.io/9217
- EPSG = European Petroleum Survey Group
"""


def pixel_to_latlon(x, y, transform, crs):
    """Convert pixel coordinates to latitude and longitude."""
    x, y = np.meshgrid(x, y)
    lon, lat = transform * (x, y)

    return lat, lon


# filename = Path('VNM_pph_v2b_2020.tif')
# filename = Path('VNM_pph_v2b_2020_UNadj.tif')
# filename = Path('VNM_ppp_v2b_2020.tif')
filename = Path('VNM_ppp_v2b_2020_UNadj.tif')
if False:
    print('Processing WorldPop population count')
    # Import
    branch_path = Path(
        'Socio-Demographic Data', 'WorldPop population count',
        'Population Counts', 'Individual countries', 'Vietnam',
        'Viet_Nam_100m_Population'
    )
    path = Path('..', 'A Collate Data', branch_path, filename)
    print(path)

    # Load the data
    with rasterio.open(path) as src:
        # Access metadata
        print(f'Width: {src.width}')
        print(f'Height: {src.height}')
        print(f'Number of bands: {src.count}')
        print(f'Coordinate reference system (CRS): {src.crs}')
        print(f'Transform:\n{src.transform}')
        # Read data from band 1
        source_data = src.read(1)
        # Get the geospatial information
        width = src.width
        height = src.height
        transform = src.transform
        crs = src.crs

    # Naive plot
    plt.imshow(source_data, cmap='GnBu')
    plt.title('GeoTIFF Band 1')
    plt.colorbar()
    # Export
    os.makedirs(branch_path, exist_ok=True)
    path = Path(branch_path, filename.stem + ' - Naive')
    plt.savefig(path)
    # Save the tick details for the next plot
    ylocs, ylabels = plt.yticks()
    xlocs, xlabels = plt.xticks()
    # Trim
    ylocs = ylocs[1:-1]
    xlocs = xlocs[1:-1]
    # Finish
    plt.close()

    # Replace placeholder numbers with 0
    # (-3.4e+38 is the smallest single-precision floating-point number)
    df = pd.DataFrame(source_data)
    source_data = df[df != -3.4028234663852886e+38]
    """
    Sanity check: calculate the total population
    Google says that Vietnam's population was 96.65 million (2020)

    VNM_pph_v2b_2020.tif
    90,049,150 (2020)

    VNM_pph_v2b_2020_UNadj.tif
    96,355,010 (2020)

    VNM_ppp_v2b_2020.tif
    90,008,170 (2020)

    VNM_ppp_v2b_2020_UNadj.tif
    96,355,000 (2020)
    """
    print(source_data.sum().sum())

    # Plot - no normalisation
    plt.imshow(source_data, cmap='GnBu')
    plt.title('GeoTIFF Band 1')
    plt.colorbar()
    # Convert pixel coordinates to latitude and longitude
    lat, lon = pixel_to_latlon(xlocs, ylocs, transform, crs)
    # Flatten into a list
    lat = [str(round(x[0], 1)) for x in lat]
    lon = [str(round(x, 1)) for x in lon[0]]
    # Convert the axis ticks from pixels into latitude and longitude
    plt.yticks(ylocs, lat)
    plt.xticks(xlocs, lon)
    # Export
    os.makedirs(branch_path, exist_ok=True)
    path = Path(branch_path, filename.stem)
    plt.savefig(path)
    plt.close()

    # # Plot - log transformed
    # source_data = np.log(source_data)
    # plt.imshow(source_data, cmap='GnBu')
    # plt.title('GeoTIFF Band 1')
    # plt.colorbar()
    # # Convert pixel coordinates to latitude and longitude
    # lat, lon = pixel_to_latlon(xlocs, ylocs, transform, crs)
    # # Flatten into a list
    # lat = [str(round(x[0], 1)) for x in lat]
    # lon = [str(round(x, 1)) for x in lon[0]]
    # # Convert the axis ticks from pixels into latitude and longitude
    # plt.yticks(ylocs, lat)
    # plt.xticks(xlocs, lon)
    # # Export
    # os.makedirs(branch_path, exist_ok=True)
    # path = Path(branch_path, filename.stem + ' - Log Scale')
    # plt.savefig(path)
    # plt.close()

#
# Geospatial and Socio-Demographic Data
#

"""
WorldPop population count
"""
if True:
    filenames = [
        # 'gadm41_VNM_0.shp',  # Takes 2.8s
        'gadm41_VNM_1.shp',  # Takes 187.2s
        # 'gadm41_VNM_2.shp',
        # 'gadm41_VNM_3.shp',
    ]
    for filename in filenames:
        out_dir = Path(
            'Geospatial and Socio-Demographic Data', 'Population',
            Path(filename).stem
        )
        os.makedirs(out_dir, exist_ok=True)

        # Import the shape file
        trunk_path = Path('..', 'A Collate Data')
        branch_path = Path(
            'Geospatial data', 'GADM administrative map', 'gadm41_VNM_shp',
        )
        leaf_path = Path(filename)
        path = Path(trunk_path, branch_path, leaf_path)
        gdf = gpd.read_file(path)

        # Import the TIFF file
        trunk_path = Path('..', 'A Collate Data')
        branch_path = Path(
            'Socio-Demographic Data', 'WorldPop population count',
            'Population Counts', 'Individual countries', 'Vietnam',
            'Viet_Nam_100m_Population'
        )
        leaf_path = Path('VNM_ppp_v2b_2020_UNadj.tif')
        path = Path(trunk_path, branch_path, leaf_path)
        src = rasterio.open(path)
        # Read data from band 1
        population_data = src.read(1)

        # Replace placeholder numbers with 0
        mask = population_data == -3.4028234663852886e+38
        population_data[mask] = 0

        # Initialise output data frame
        output = pd.DataFrame()
        # Iterate over the regions in the shape file
        total_pop = 0
        for i, row in gdf.iterrows():
            # Initialise new row
            new_row = {}
            # Populate the new row
            new_row['country'] = row['COUNTRY']
            title = row['COUNTRY']
            if 'NAME_1' in list(gdf):
                new_row['name_1'] = row['NAME_1']
                title = row['NAME_1']
            if 'NAME_2' in list(gdf):
                new_row['name_2'] = row['NAME_2']
                title = row['NAME_2']
            if 'NAME_3' in list(gdf):
                new_row['name_3'] = row['NAME_3']
                title = row['NAME_3']
            # Create a mask for the main landmass
            mask = geometry_mask(
                [row['geometry']], out_shape=population_data.shape,
                transform=src.transform, invert=True
            )
            # Use the mask to extract the region
            region_population_data = population_data * mask
            # Sum the pixel values to get the population for the region
            region_population = region_population_data.sum()
            print(title, region_population)
            total_pop += region_population
            # Add to output data frame
            new_row['population'] = region_population
            new_row_df = pd.DataFrame(new_row, index=[1])
            output = pd.concat([output, new_row_df], ignore_index=True)

            # Don't create a plot for the whole country
            if title == 'Vietnam':
                break

            # Plot
            fig = plt.figure(figsize=(10, 10))
            ax = plt.axes()
            df = pd.DataFrame(region_population_data)
            df = df.replace(0, np.nan)
            df = df.dropna(how='all', axis=0)
            df = df.dropna(how='all', axis=1)
            # df = df[df != 0]
            # df = np.log(df)
            img = ax.imshow(df, cmap='GnBu')
            plt.colorbar(img, label='Population')

            # Define the desired bounds (xmin, ymin, xmax, ymax)
            desired_bounds = (0, df.shape[0], df.shape[1], 0)
            polygon = row['geometry']
            if polygon.geom_type == 'MultiPolygon':
                for sub_polygon in polygon.geoms:
                    # Calculate scaling factors for the x and y dimensions
                    x_scale = (desired_bounds[2] - desired_bounds[0]) / (sub_polygon.bounds[2] - sub_polygon.bounds[0])
                    y_scale = (desired_bounds[3] - desired_bounds[1]) / (sub_polygon.bounds[3] - sub_polygon.bounds[1])
                    # Scale the polygon using the calculated factors
                    scaled_polygon = Polygon([(
                        x * x_scale + desired_bounds[0] - (sub_polygon.bounds[0] * x_scale),
                        y * y_scale + desired_bounds[1] - (sub_polygon.bounds[1] * y_scale)
                    ) for x, y in sub_polygon.exterior.coords])
                    # Plot the scaled polygon
                    gpd.GeoSeries([scaled_polygon]).plot(
                        ax=ax, facecolor='none', edgecolor='k', linewidth=1
                    )
            else:
                # Calculate scaling factors for the x and y dimensions
                x_scale = (desired_bounds[2] - desired_bounds[0]) / (polygon.bounds[2] - polygon.bounds[0])
                y_scale = (desired_bounds[3] - desired_bounds[1]) / (polygon.bounds[3] - polygon.bounds[1])
                # Scale the polygon using the calculated factors
                scaled_polygon = Polygon([(
                    x * x_scale + desired_bounds[0] - (polygon.bounds[0] * x_scale),
                    y * y_scale + desired_bounds[1] - (polygon.bounds[1] * y_scale)
                ) for x, y in polygon.exterior.coords])
                # Plot the scaled polygon
                gpd.GeoSeries([scaled_polygon]).plot(
                    ax=ax, facecolor='none', edgecolor='k', linewidth=1
                )
            ax.set_title(title)
            # Export
            path = Path(out_dir, title)
            plt.savefig(path)
            plt.close()

        # Export
        print(total_pop)
        path = Path(out_dir, Path(filename).stem + '.csv')
        output.to_csv(path, index=False)

"""
Sanity checking:

print(region_populations)
[('Vietnam', 96355304.0)]
print(population_data.sum())
96355090.0
"""
