# Author: Prathyush Sambaturu
# Purpose: Python script to preprocess and aggregate relative wealth index scores for administrative regions (admin2 or admin3)
# of Vietnam. The code for aggregated is adapted from the following tutorial:
# https://dataforgood.facebook.com/dfg/docs/tutorial-calculating-population-weighted-relative-wealth-index. 

#Load necessary packages
import pandas as pd
import matplotlib.pyplot as plt
import descartes
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import contextily
import numpy as np
from pyquadkey2 import quadkey

# Function takes a shapefile of administrative regions of a country as a geopandas dataframe and
# create a dictionary of polygons where the key is the Id of the polygon and the value is its geometry
def get_polygons_from_shapefile(shapefile, admin_geoid):
        """
                @param shapefile: geodataframe
                @param admin_geoid: str
                @return polygons: dict
        """
        polygons = dict(zip(shapefile[admin_geoid], shapefile['geometry']))
        return polygons

# Function to take a path to csv file relative wealth index
def get_rwi_dataframe_from_csv(rwi_csv_file):
	"""
		@param rwi_csv_file: str
		@return rwi: dataframe
	"""
	rwi = pd.read_csv(rwi_csv_file)
	rwi['geo_id'] = rwi.apply(lambda x: get_point_in_polygon(x['latitude'], x['longitude'], polygons), axis=1)
	rwi = rwi[rwi['geo_id'] != 'null']
	return rwi

# Function to take a csv file with population data from Meta and generates a dataframe with total population for tiles 
# of zoom level 14 (Bing tiles) using quadkeys
def get_bing_tile_z14_pop(pop_file):
	"""
		@param pop_file: str
		@return bing_tile_z14_pop: dataframe
	"""
	population = pd.read_csv(pop_file)
	population = population.rename(columns={'vnm_general_2020': 'pop_2020'})
	population['quadkey'] = population.apply(lambda x: str(quadkey.from_geo((x['latitude'], x['longitude']), 14)), axis=1)
	bing_tile_z14_pop = population.groupby('quadkey', as_index=False)['pop_2020'].sum()
	bing_tile_z14_pop["quadkey"]=bing_tile_z14_pop["quadkey"].astype(np.int64)
	return bing_tile_z14_pop

# Function to return the id of administrative region in which the center (given by latitude and longitude) of a 
# 2.4km^2 gridcell. This function is from the tutorial 
def get_point_in_polygon(lat, lon, polygons):
	""" 
		@param lat: double
        	@param lon: double
        	@param polygons: dict
        	@return geo_id: str
	"""
	point = Point(lon, lat)
	for geo_id in polygons:
		polygon = polygons[geo_id]
		if polygon.contains(point):
			return geo_id
	return 'null'


shpfile = '/Users/user/Documents/Codes/Vietnam Data Streams Processing/DART-Pipeline/data/gadm41_VNM_shp/gadm41_VNM_2.shp'
rwifile = '/Users/user/Documents/Codes/Vietnam Data Streams Processing/DART-Pipeline/data/vnm_relative_wealth_index.csv'
popfile = '/Users/user/Documents/Codes/Vietnam Data Streams Processing/DART-Pipeline/data/vnm_general_2020.csv'

shapefile = gpd.read_file(shpfile)
polygons = get_polygons_from_shapefile(shapefile, 'GID_2')
rwi = get_rwi_dataframe_from_csv(rwifile)
bing_tile_z14_pop = get_bing_tile_z14_pop(popfile)

shapefile = gpd.read_file(shpfile)
rwi_pop = rwi.merge(bing_tile_z14_pop[['quadkey', 'pop_2020']], on='quadkey', how='inner')
geo_pop = rwi_pop.groupby('geo_id', as_index=False)['pop_2020'].sum()
geo_pop = geo_pop.rename(columns={'pop_2020': 'geo_2020'})
rwi_pop = rwi_pop.merge(geo_pop, on='geo_id', how='inner')
rwi_pop['pop_weight'] = rwi_pop['pop_2020'] / rwi_pop['geo_2020']
rwi_pop['rwi_weight'] = rwi_pop['rwi'] * rwi_pop['pop_weight']
geo_rwi = rwi_pop.groupby('geo_id', as_index=False)['rwi_weight'].sum()
shapefile_rwi = shapefile.merge(geo_rwi, left_on='GID_2', right_on='geo_id')

fig, ax = plt.subplots(figsize=(15,12))
shapefile_rwi.plot(ax=ax, column = 'rwi_weight', marker = 'o', markersize=1,legend=True, label='RWI score')
contextily.add_basemap(ax,crs={'init':'epsg:4326'},source=contextily.providers.OpenStreetMap.Mapnik)
plt.title('Relative Wealth Index scores of admin3 regions in Vietnam')
plt.legend()
plt.savefig('rwi_weight_admin3.png', dpi=600)
