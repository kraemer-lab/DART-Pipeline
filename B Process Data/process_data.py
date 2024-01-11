"""
Script to process raw data that has already been collated.

After the `collate_data.py` script in the "A Collate Data" folder has been run,
the `process_data.py` script in the "B Process Data" folder can be run. This
script has been tested on Python 3.12 and more versions will be tested in the
future.

**Installation and Setup**

As with the A-script, it is recommended to work in a Python virtual
environment specific to this script. Open a terminal in the "B Process Data"
folder and run the following:

.. code-block::

    $ python3 -m venv venv
    $ source venv/bin/activate

The package requirements for the B-script are listed in `requirements.txt` -
install these dependencies by running the following:

.. code-block::

    $ python3 -m pip install -r requirements.txt

Additionally, on macOS, the Geospatial Data Abstraction Library needs to be
installed from Homebrew:

.. code-block::

    $ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    $ brew --version
    $ brew install gdal
    $ ogr2ogr --version

**Example Usage**

To process GADM administrative map geospatial data, run one or more of the
following commands (depending on the administrative level you are interested
in, a parameter controlled by the `-a` flag):

.. code-block::

    $ python3 process_data.py --data_name "GADM administrative map" # Approx run time: 0m1.681s
    $ python3 process_data.py --data_name "GADM administrative map" -a 1  # Approx run time: 0m5.659s
    $ python3 process_data.py --data_name "GADM administrative map" -a 2  # Approx run time: 0m50.393s
    $ python3 process_data.py --data_name "GADM administrative map" -a 3  # Approx run time: 8m54.418s

These commands will create a "Geospatial Data" sub-folder and output data into
it.

In general, use `EPSG:9217 <https://epsg.io/9217>`_ or
`EPSG:4326 <https://epsg.io/4326>`_ for map projections and use the
`ISO 3166-1 alpha-3 <https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3>`_
format for country codes.
"""
# Create the requirements file with:
# $ python3 -m pip install pipreqs
# $ pipreqs '.' --force
from pathlib import Path
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as mticker
import os
import numpy as np
import json
from shapely.geometry import Point, Polygon
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
import argparse


def get_base_directory(path='.'):
    """
    Get the base directory for a Git project.

    Parameters
    ----------
    path : str or pathlib.Path, default '.'
        The path to the child directory.

    Returns
    -------
    str or pathlib.Path, or None
        The path to the parent/grand-parent/etc directory of the child
        directory that contains the ".git" folder. If no such directory exists,
        returns None.
    """
    path = os.path.abspath(path)
    while True:
        if '.git' in os.listdir(path):
            return path
        if path == os.path.dirname(path):
            return None  # If the current directory is the root, break the loop
        path = os.path.dirname(path)


def plot_pop_density(df, folderpath, filename):
    """
    Plot the population for a region.

    Parameters
    ----------
    df : DataFrame
        The data to plot.
    folderpath : str or pathlib.Path
        The parent directory for the output plot.
    filename : str or pathlib.Path
        The filename for the output plot.
    """
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


def pixel_to_latlon(x, y, transform, crs):
    """
    Convert pixel coordinates to latitude and longitude.

    Parameters
    ----------
    x, y : list
        The x- and y-locations of the pixels to be converted to latitude and
        longitude.
    transform : Affine
        Affine transformation matrix as given in the GeoTIFF file.
    crs : rasterio.crs
        A Rasterio coordinate reference system.

    Returns
    -------
    lat, lon : array
        The latitude and longitude coordinates.
    """
    x, y = np.meshgrid(x, y)
    lon, lat = transform * (x, y)

    return lat, lon


# Establish the base directory
path = Path(__file__)
base_dir = get_base_directory(path.parent)

# If running directly
if __name__ == "__main__":
    # Create command-line argument parser
    desc = 'Process data that has been previously downloaded and collated.'
    parser = argparse.ArgumentParser(description=desc)

    # Add optional arguments: data_name
    message = 'The name of the data field to be processed.'
    # default = 'GADM administrative map'
    # default = 'WorldPop population density'
    # default = 'WorldPop population count'
    # default = 'GADM administrative map and WorldPop population count'
    # default = 'GADM administrative map and WorldPop population density'
    default = ''
    parser.add_argument('--data_name', '-n', default=default, help=message)

    # Add optional arguments: admin_level
    message = '''Some data fields have different data for each administrative
    level'''
    default = '1'
    parser.add_argument('--admin_level', '-a', default=default, help=message)

    # Add optional arguments: year
    message = '''Some data fields have data available for multiple years.'''
    default = ''
    parser.add_argument('--year', '-y', default=default, help=message)

    # Add optional arguments: resolution_type
    message = '''"ppp" (people per pixel) or "pph" (people per hectare).'''
    default = 'ppp'
    parser.add_argument(
        '--resolution_type', '-r', default=default, help=message
    )

    # Add optional arguments: country_iso3
    message = '''Country code in "ISO 3166-1 alpha-3" format.'''
    default = 'VNM'
    parser.add_argument('--country_iso3', '-c', default=default, help=message)

    # Parse arguments from terminal
    args = parser.parse_args()
# If running via Sphinx
else:
    # Create a fake args object
    args = lambda: None
    args.data_name = ''

# Check
if True:
    print('Arguments:')
    for arg in vars(args):
        print(f'{arg + ":":20s} {vars(args)[arg]}')

data_name_to_type = {
    'APHRODITE Daily mean temperature product (V1808)': 'Meteorological Data',
    'APHRODITE Daily accumulated precipitation (V1901)': 'Meteorological Data',
    'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations':
    'Meteorological Data',
    'TerraClimate gridded temperature, precipitation, and other':
    'Meteorological Data',
    'ERA5 atmospheric reanalysis': 'Meteorological Data',
    'WorldPop population density': 'Socio-Demographic Data',
    'WorldPop population count': 'Socio-Demographic Data',
    'GADM administrative map': 'Geospatial Data',
}


"""
Geospatial data
 └ GADM administrative map

Run times:

- `time python3.12 process_data.py`: 0m1.681s
- `time python3.12 process_data.py -a 1`: 0m5.659s
- `time python3.12 process_data.py -a 2`: 0m50.393s
- `time python3.12 process_data.py -a 3`: 8m54.418s
"""
if args.data_name == 'GADM administrative map':
    filenames = [f'gadm41_VNM_{args.admin_level}.shp']
    for filename in filenames:
        filename = Path(filename)
        relative_path = Path(
            'Geospatial Data', 'GADM administrative map',
            'gadm41_VNM_shp'
        )
        # Create output directory
        out_dir = Path(base_dir, 'B Process Data', relative_path)
        out_dir.mkdir(parents=True, exist_ok=True)

        # Import the shape file
        path = Path(base_dir, 'A Collate Data', relative_path, filename)
        gdf = gpd.read_file(path)

        # Plot
        fig = plt.figure(figsize=(10, 10))
        ax = plt.axes()
        gdf.plot(ax=ax)
        plt.title(filename)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        # Export
        path = Path(base_dir, 'B Process Data', relative_path, filename.stem)
        plt.savefig(path)
        plt.close()

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

            # Plot
            fig = plt.figure()
            ax = plt.axes()
            if row['geometry'].geom_type == 'MultiPolygon':
                for polygon in row['geometry'].geoms:
                    x, y = polygon.exterior.xy
                    plt.plot(x, y)
            elif row['geometry'].geom_type == 'Polygon':
                x, y = row['geometry'].exterior.xy
                plt.plot(x, y)
            ax.set_aspect('equal')
            plt.title(title)
            plt.xlabel('Longitude')
            plt.ylabel('Latitude')
            # Export
            folderpath = Path(
                base_dir, 'B Process Data', relative_path, filename.stem
            )
            os.makedirs(folderpath, exist_ok=True)
            filepath = Path(folderpath, title)
            plt.savefig(filepath)
            plt.close()

"""
Socio-demographic data
 └ WorldPop population density

Run times:

- `time python3.12 process_data.py --data_name "WorldPop population density"`:
  0m2.420s
"""
if args.data_name == 'WorldPop population density':
    relative_path = Path(
        'Socio-Demographic Data', 'WorldPop population density',
        'Population Density',
        'Unconstrained individual countries UN adjusted (1km resolution)',
        'Vietnam'
    )

    # Create output directory
    out_dir = Path(base_dir, 'B Process Data', relative_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Import the population density data for Vietnam
    filename = Path('vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip')
    path = Path(base_dir, 'A Collate Data', relative_path, filename)
    df = pd.read_csv(path)

    # Export as-is
    filename = Path(filename.stem + '.csv')
    path = Path(base_dir, 'B Process Data', relative_path, filename)
    df.to_csv(path, index=False)

    # Plot
    fig = plt.figure()
    ax = plt.axes()
    pt = df.pivot_table(index='Y', columns='X', values='Z')
    ax.imshow(pt, cmap='viridis')
    ax.invert_yaxis()
    plt.title('Population Density')
    # Export
    path = Path(
        base_dir, 'B Process Data', relative_path, filename.stem
    )
    plt.savefig(path)
    plt.close()

"""
Socio-demographic data
 └ WorldPop population count

- EPSG:9217: https://epsg.io/9217
- EPSG:4326: https://epsg.io/4326
- EPSG = European Petroleum Survey Group
"""
if args.data_name == 'WorldPop population count':
    print('Processing WorldPop population count')

    # Get the year for which data will be loaded
    print(args.year)
    if args.year == '':
        year = '2020'
    else:
        year = args.year

    # Get the other arguments
    iso3 = args.country_iso3
    rt = args.resolution_type

    # filename = Path('VNM_pph_v2b_2020.tif')
    # filename = Path('VNM_pph_v2b_2020_UNadj.tif')
    # filename = Path('VNM_ppp_v2b_2020.tif')
    # filename = Path('VNM_ppp_v2b_2020_UNadj.tif')
    filename = Path(f'{iso3}_{rt}_v2b_{year}_UNadj.tif')

    # Import
    relative_path = Path(
        'Socio-Demographic Data', 'WorldPop population count',
        'Population Counts', 'Individual countries', 'Vietnam',
        'Viet_Nam_100m_Population'
    )
    path = Path(base_dir, 'A Collate Data', relative_path, filename)
    # Load the data
    src = rasterio.open(path)
    # Access metadata
    width = src.width
    print(f'Width: {width}')
    height = src.height
    print(f'Height: {height}')
    # Get the coordinate reference system (CRS) of the GeoTIFF
    crs = src.crs
    print(f'Coordinate reference system (CRS): {crs}')
    # Get the affine transformation coefficients
    transform = src.transform
    print(f'Transform:\n{transform}')
    # Read data from band 1
    print(f'Number of bands: {src.count}')
    source_data = src.read(1)

    # Naive plot
    plt.imshow(source_data, cmap='GnBu')
    plt.title('WorldPop Population Count')
    plt.colorbar(shrink=0.8, label='Population')
    # Export
    fn = filename.stem + ' - Naive.png'
    path = Path(base_dir, 'B Process Data', relative_path, fn)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(path)

    # Save the tick details for the next plot
    ylocs, ylabels = plt.yticks()
    xlocs, xlabels = plt.xticks()
    # Trim
    ylocs = ylocs[1:-1]
    xlocs = xlocs[1:-1:2]
    # Finish
    plt.close()

    # Replace placeholder numbers with 0
    # (-3.4e+38 is the smallest single-precision floating-point number)
    df = pd.DataFrame(source_data)
    population_data = df[df != -3.4028234663852886e+38]
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
    print(f'Population as per {filename}: {population_data.sum().sum()}')

    # Plot - no normalisation
    fn = filename.stem + '.png'
    path = Path(base_dir, 'B Process Data', relative_path, fn)
    if not path.exists():
        plt.imshow(population_data, cmap='GnBu')
        plt.title('WorldPop Population Count')
        plt.colorbar(shrink=0.8, label='Population')
        plt.ylabel('Latitude')
        plt.xlabel('Longitude')
        # Convert pixel coordinates to latitude and longitude
        lat, lon = pixel_to_latlon(xlocs, ylocs, transform, crs)
        # Flatten into a list
        lat = [str(round(x[0], 1)) for x in lat]
        lon = [str(round(x, 1)) for x in lon[0]]
        # Convert the axis ticks from pixels into latitude and longitude
        plt.yticks(ylocs, lat)
        plt.xticks(xlocs, lon)
        # Export
        plt.savefig(path)
        plt.close()

    # Plot - log transformed
    fn = filename.stem + ' - Log Scale.png'
    path = Path(base_dir, 'B Process Data', relative_path, fn)
    if not path.exists():
        population_data = np.log(population_data)
        plt.imshow(population_data, cmap='GnBu')
        plt.title('WorldPop Population Count')
        plt.colorbar(shrink=0.8, label='Population (log)')
        plt.ylabel('Latitude')
        plt.xlabel('Longitude')
        # Convert pixel coordinates to latitude and longitude
        lat, lon = pixel_to_latlon(xlocs, ylocs, transform, crs)
        # Flatten into a list
        lat = [str(round(x[0], 1)) for x in lat]
        lon = [str(round(x, 1)) for x in lon[0]]
        # Convert the axis ticks from pixels into latitude and longitude
        plt.yticks(ylocs, lat)
        plt.xticks(xlocs, lon)
        # Export
        plt.savefig(path)
        plt.close()

    # Convert pixel coordinates to latitude and longitude
    cols = np.arange(source_data.shape[1])
    lon, _ = rasterio.transform.xy(transform, (1,), cols)
    rows = np.arange(source_data.shape[0])
    _, lat = rasterio.transform.xy(transform, rows, (1,))
    # Replace placeholder numbers with 0
    mask = source_data == -3.4028234663852886e+38
    source_data[mask] = 0
    # Create a DataFrame with latitude, longitude, and pixel values
    df = pd.DataFrame(source_data, index=lat, columns=lon)
    # Export
    fn =  filename.stem + '.csv'
    path = Path(base_dir, 'B Process Data', relative_path, fn)
    if not path.exists():
        print(f'Exporting "{path}"')
        df.to_csv(path)
    # Sanity checking
    if filename.stem == 'VNM_ppp_v2b_2020_UNadj':
        assert df.to_numpy().sum() == 96355088.0  # 96,355,088

    # Plot - transformed
    fn = filename.stem + ' - Transformed.png'
    path = Path(base_dir, 'B Process Data', relative_path, fn)
    if not path.exists():
        plt.imshow(df, cmap='GnBu')
        plt.title('WorldPop Population Count')
        plt.colorbar(shrink=0.8, label='Population')
        plt.savefig(path)
        plt.close()

"""
Geospatial and Socio-Demographic Data
 └ GADM administrative map and WorldPop population count
"""
if args.data_name == 'GADM administrative map and WorldPop population count':
    # Get the year for which data will be loaded
    if args.year == '':
        year = '2020'
    else:
        year = args.year

    # Get the other arguments
    admin_level = args.admin_level
    iso3 = args.country_iso3
    rt = args.resolution_type

    # Import the TIFF file
    relative_path = Path(
        'Socio-Demographic Data', 'WorldPop population count',
        'Population Counts', 'Individual countries', 'Vietnam',
        'Viet_Nam_100m_Population'
    )
    filename = Path(f'{iso3}_{rt}_v2b_{year}_UNadj.tif')
    path = Path(base_dir, 'A Collate Data', relative_path, filename)
    src = rasterio.open(path)
    # Get the coordinate reference system (CRS) of the GeoTIFF
    crs = src.crs
    print('Coordinate Reference System (CRS) of the GeoTIFF file:', crs)
    # Read data from band 1
    population_data = src.read(1)
    # Replace placeholder numbers with 0
    mask = population_data == -3.4028234663852886e+38
    population_data[mask] = 0
    # Sanity checking
    assert population_data.sum() == 96355088.0, \
        f'{population_data.sum()} != 96355088.0'  # 96,355,088

    # Import the shape file
    relative_path = Path(
        'Geospatial Data', 'GADM administrative map', 'gadm41_VNM_shp',
    )
    filename = f'gadm41_VNM_{admin_level}.shp'
    path = Path(base_dir, 'A Collate Data', relative_path, filename)
    gdf = gpd.read_file(path)
    # Get the coordinate reference system (CRS) of the GeoDataFrame
    crs = gdf.crs
    print('Coordinate Reference System (CRS) of the shapefile:', crs)

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
        # Look at the polygons in the shapefile
        mask = geometry_mask(
            [row['geometry']], out_shape=population_data.shape,
            transform=src.transform, invert=True
        )
        # Use the mask to extract the region
        region_population_data = population_data * mask
        # Sum the pixel values to get the population for the region
        region_population = region_population_data.sum()
        print(title, region_population)
        # Add to output data frame
        new_row['population'] = region_population
        new_row_df = pd.DataFrame(new_row, index=[1])
        output = pd.concat([output, new_row_df], ignore_index=True)
        # Add to running count
        total_pop += region_population

        # Plot
        fig = plt.figure(figsize=(10, 10))
        ax = plt.axes()
        if title == 'Vietnam':
            arr = region_population_data
            arr[arr == 0] = np.nan
            img = ax.imshow(arr, cmap='GnBu')
        else:
            df = pd.DataFrame(region_population_data)
            df = df.replace(0, np.nan)
            df = df.dropna(how='all', axis=0)
            df = df.dropna(how='all', axis=1)
            img = ax.imshow(df, cmap='GnBu')
        plt.colorbar(img, label='Population', shrink=0.8)
        ax.set_title(title)
        # Export
        relative_path = Path(
            'Geospatial and Socio-Demographic Data',
            'GADM administrative map and WorldPop population count',
            filename.removesuffix(Path(filename).suffix)
        )
        path = Path(base_dir, 'B Process Data', relative_path, title)
        os.makedirs(path.parent, exist_ok=True)
        plt.savefig(path)
        plt.close()

    # Export
    print(f'Total population: {total_pop}')
    path = Path(base_dir, 'B Process Data', relative_path, 'Population.csv')
    output.to_csv(path, index=False)

"""
Geospatial and Socio-Demographic Data
 └ GADM administrative map and WorldPop population density

Run times:

- If the labelled population density data does not exist:
    - `time python3.12 process_data.py -n "GADM administrative map and WorldPop
    population density"`: 31m52.408s
- If the labelled population density data exists:
    - `time python3.12 process_data.py -n "GADM administrative map and WorldPop
    population density"`: 0m16.145s
"""
if args.data_name == 'GADM administrative map and WorldPop population density':
    # Get the year for which data will be loaded
    if args.year == '':
        year = '2020'
    else:
        year = args.year

    # Get the admin level
    print('Only admin level 2 is currently implemented')
    admin_level = args.admin_level
    admin_level = 2

    # Get the other argument
    iso3 = args.country_iso3

    # Import the population density data for Vietnam
    relative_path = Path(
        'Socio-Demographic Data', 'WorldPop population density',
        'Population Density',
        'Unconstrained individual countries UN adjusted (1km resolution)',
        'Vietnam'
    )
    filename = Path(f'{iso3.lower()}_pd_{year}_1km_UNadj_ASCII_XYZ.zip')
    path = Path(base_dir, 'A Collate Data', relative_path, filename)
    df = pd.read_csv(path)

    # Import the coordinates of the borders of Vietnam's regions
    relative_path = Path('Geospatial Data', 'GADM administrative map')
    filename = f'gadm41_{iso3}_{admin_level}.json'
    path = Path(base_dir, 'A Collate Data', relative_path, filename)
    with open(path) as file:
        geojson = json.load(file)

    # Check if the labelled population density data exists
    relative_path = Path(
        'Geospatial and Socio-Demographic Data',
        'GADM administrative map and WorldPop population density'
    )
    filename = 'Vietnam.csv'
    path = Path(base_dir, 'B Process Data', relative_path, filename)
    if path.exists():
        # Import the labelled data
        df = pd.read_csv(path)

        # Plot whole country
        plot_pop_density(df, path.parent, 'Vietnam.png')

        # Initialise output dictionaries
        dct_admin_2 = {}
        dct_admin_3 = {}

        # Create the output folders
        path = Path(path.parent, f'Admin 2')
        os.makedirs(path, exist_ok=True)

        # Analyse each province/city
        for admin_2 in df[f'Admin 2'].unique():
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
        filename = 'WorldPop population density - Admin 2.json'
        path = Path(base_dir, 'B Process Data', relative_path, filename)
        with open(path, 'w') as file:
            json.dump(dct_admin_2, file)
        filename = 'WorldPop population density - Admin 3.json'
        path = Path(base_dir, 'B Process Data', relative_path, filename)
        with open(path, 'w') as file:
            json.dump(dct_admin_3, file)
    else:
        # Classify the location of each coordinate
        df['Country'] = None
        df['Admin 2'] = None
        df['Admin 3'] = None
        # Pre-construct the polygons
        polygons = []
        regions = []
        for feature in geojson['features']:
            polygon = Polygon(feature['geometry']['coordinates'][0][0])
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
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
