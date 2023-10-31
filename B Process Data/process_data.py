"""
Process data.

$ python3.11 -m pip install shapely
$ python3.11 -m pip install --upgrade pip
$ python3.11 -m pip install geopandas$ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
$ brew --version
$ brew install gdal
$ ogr2ogr --version
"""
from pathlib import Path
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
import matplotlib.ticker as mticker
import os
import numpy as np
import json
from shapely.geometry import Point, Polygon
import geopandas as gpd


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
# Socio-demographic data
#

"""
WorldPop population density
"""
# Create the output folder
out_folder = Path(
    '02 Processed Data', 'Socio-Demographic Data',
    'WorldPop population density'
)
os.makedirs(out_folder, exist_ok=True)

# # Import the population density data for Vietnam
# path = Path(
#     '01 Collated Data', 'Socio-Demographic Data',
#     'WorldPop population density', 'Population Density',
#     'Unconstrained individual countries UN adjusted (1km resolution)',
#     'Vietnam', 'vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip'
# )
# df = pd.read_csv(path)

# Import the coordinates of the borders of Vietnam's regions
path = Path('01 Collated Data', 'VNM_ADM_2.geojson')
with open(path) as file:
    geojson = json.load(file)

# # Classify the location of each coordinate
# df['Country'] = None
# df['Admin 2'] = None
# df['Admin 3'] = None
# # Pre-construct the polygons
# polygons = []
# regions = []
# for feature in geojson['features']:
#     polygon = Polygon(feature['geometry']['coordinates'][0][0])
#     print(polygon)
#     polygons.append(polygon)
#     region = (
#         feature['properties']['COUNTRY'],
#         feature['properties']['NAME_1'],
#         feature['properties']['NAME_2']
#     )
#     regions.append(region)
# # Iterate over the coordinates
# for i, row in df.iterrows():
#     # Update the user
#     if i % 100 == 0:
#         print(f'{i} / {len(df)}')
#     point = Point(df.loc[i, 'X'], df.loc[i, 'Y'])
#     for j, polygon in enumerate(polygons):
#         # Check if the coordinate is in this region
#         if point.within(polygon):
#             # We have found the region this coordinate is in
#             df.loc[i, 'Country'] = regions[j][0]
#             df.loc[i, 'Admin 2'] = regions[j][1]
#             df.loc[i, 'Admin 3'] = regions[j][2]
#             # Move to the next line of the data frame
#             break
# # Export
# path = Path(out_folder, 'Vietnam.csv')
# df.to_csv(path, index=False)
# Import
path = Path(out_folder, 'Vietnam.csv')
df = pd.read_csv(path)

# Plot whole country
plot_pop_density(df, out_folder, 'Vietnam.png')

# # Initialise output dictionaries
# dct_admin_2 = {}
# dct_admin_3 = {}

# # Create the output folders
# path = Path(out_folder, 'Admin 2')
# os.makedirs(path, exist_ok=True)

# # Analyse each province/city
# for admin_2 in df['Admin 2'].unique():
#     if admin_2 is not np.nan:
#         subset = df[df['Admin 2'] == admin_2].copy()
#         print(admin_2)
#         # plot_pop_density(subset, path, f'{admin_2}.png')
#         dct_admin_2[admin_2] = subset['Z'].mean()
#         for admin_3 in subset['Admin 3'].unique():
#             if admin_3 is not np.nan:
#                 ssubset = subset[subset['Admin 3'] == admin_3].copy()
#                 dct_admin_3[admin_3] = ssubset['Z'].mean()
# # Export
# path = Path(out_folder, 'WorldPop population density - Admin 2.json')
# with open(path, 'w') as file:
#     json.dump(dct_admin_2, file)
# path = Path(out_folder, 'WorldPop population density - Admin 3.json')
# with open(path, 'w') as file:
#     json.dump(dct_admin_3, file)

#
# Plot using a shape file
#

# Set SHAPE_RESTORE_SHX to YES
os.environ['SHAPE_RESTORE_SHX'] = 'YES'

# Import the shape file
path = Path('01 Collated Data', 'vietnam_adm3.shp')
gdf = gpd.read_file(path)
print(gdf.head())
print(gdf['geometry'][0])

# gdf = gpd.read_file('file.geojson')
# gdf.to_file('file.shp')

# Get the names of the regions
geojson_data = []
for feature in geojson['features']:
    polygon = Polygon(feature['geometry']['coordinates'][0][0])
    country = feature['properties']['COUNTRY']
    admin_2 = feature['properties']['NAME_1']
    admin_3 = feature['properties']['NAME_2']
    geojson_data.append((polygon, country, admin_2, admin_3))

gdf['Admin 2'] = None
gdf['Admin 3'] = None
for i, row in gdf[:2].iterrows():
    polygon_1 = row['geometry']
    overlaps = []
    for data in geojson_data:
        polygon_2 = data[0]
        overlap = polygon_1.intersection(polygon_2)
        overlaps.append(overlap.area)
        print(data[3], overlap.area)
    max_overlap = geojson_data[np.argmax(overlaps)]
    gdf.loc[i, 'Admin 2'] = max_overlap[2]
    gdf.loc[i, 'Admin 3'] = max_overlap[3]
print(gdf.head())

# # Define a dictionary to map polygon identifiers to colors
# color_mapping = {
#     0: 'red',
#     1: 'green',
#     2: 'blue',
#     # Add more polygons and colors as needed
# }
# # Create a custom color column in the GeoDataFrame
# gdf['custom_color'] = gdf.index.map(color_mapping)
# gdf['custom_color'] = gdf['custom_color'].fillna('black')
# print(gdf.head())

# # Plot
# A = 3  # We want figures to be A3
# figsize = (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
# fig = plt.figure(figsize=figsize, dpi=300)
# ax = plt.axes()
# # Plot the polygons with custom colors
# gdf.plot(ax=ax, color=gdf['custom_color'])
# # Customize the plot
# ax.set_title('Population Density')
# # Export
# plt.savefig('Shape.png')
