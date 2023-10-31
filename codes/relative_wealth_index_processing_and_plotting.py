#Author: Prathyush Sambaturu
#Purpose: To process the relative wealth index file (.csv format) and plot the rwi scores for each location given by latitude and longitude.

#Load the necessary packages
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point
import contextily
import sys

#Assign values to variables using arguments from command line
input_file = sys.argv[1]
plot_path = sys.argv[2]

#Read the input file as a dataframe and convert it to a geodataframe
df = pd.read_csv(input_file)
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))

fig, ax = plt.subplots(figsize=(15,12))
gdf.plot(ax=ax, column = 'rwi', marker = 'o', markersize=1, label = 'RWI score', legend=True)
contextily.add_basemap(ax,crs={'init':'epsg:4326'},source=contextily.providers.OpenStreetMap.Mapnik)
plt.title('Relative Wealth Index scores of locations in Vietnam')
plt.legend()
plt.savefig(plot_path, dpi=600)
 


