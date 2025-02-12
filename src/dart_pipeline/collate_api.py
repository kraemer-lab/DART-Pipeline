"""
Collate module for API-based retrievals.

These require direct downloads to file.
"""
import logging

import cdsapi

from .types import PartialDate
from .util import source_path


def download_era5_reanalysis_data(dataset: str, partial_date: str):
    """
    Download ERA5 atmospheric reanalysis data.

    How to use the Climate Data Store (CDS) Application Program Interface
    (API): https://cds.climate.copernicus.eu/how-to-api

    A Climate Data Store account is needed, see
    https://pypi.org/project/cdsapi/

    Downloadable datasets:

    - `'satellite-sea-ice-thickness'`: Sea ice thickness monthly gridded data
      for the Arctic from 2002 to present
    - `'derived-era5-land-daily-statistics'`: ERA5-Land post-processed daily
      statistics from 1950 to present
    - `'reanalysis-era5-complete'`: Complete ERA5 global atmospheric reanalysis
    - `'reanalysis-era5-single-levels'`: ERA5 hourly data on single levels from
      1940 to present
    """
    source = 'meteorological/era5-reanalysis'
    logging.info('dataset:%s', dataset)
    pdate = PartialDate.from_string(partial_date)
    logging.info('pdate:%s', pdate)

    # Sea ice thickness monthly gridded data for the Arctic from 2002 to
    # present derived from satellite observations
    # https://cds.climate.copernicus.eu/datasets/satellite-sea-ice-thickness
    if dataset == 'satellite-sea-ice-thickness':
        if pdate.year in [2021, 2022, 2023]:
            satellite = ['cryosat_2']
            cdr_type = ['icdr']
        elif pdate.year in [2020]:
            satellite = ['cryosat_2']
            cdr_type = ['cdr', 'icdr']
        elif pdate.year in [
            2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019
        ]:
            satellite = ['cryosat_2']
            cdr_type = ['cdr']
        elif pdate.year in [2010]:
            satellite = ['envisat', 'cryosat_2']
            cdr_type = ['cdr']
        elif pdate.year in [2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009]:
            satellite = ['envisat']
            cdr_type = ['cdr']
        else:
            satellite = ['envisat', 'cryosat_2']
            cdr_type = ['cdr', 'icdr']
        request = {
            'satellite': satellite,
            'cdr_type': cdr_type,
            'variable': 'all',
            'year': [str(pdate.year)],
            'month': [
                '01', '02', '03',
                '04', '10', '11',
                '12'
            ],
            'version': '3_0'
        }
        path = source_path(source, f'{dataset}_{pdate.year}.nc')
        logging.info('creating:%s', path.parent)
        path.parent.mkdir(parents=True, exist_ok=True)

    # ERA5-Land post-processed daily statistics from 1950 to present
    # https://cds.climate.copernicus.eu/datasets/derived-era5-land-daily-statistics
    if dataset == 'derived-era5-land-daily-statistics':
        if pdate.month is None:
            raise ValueError('A month and a day are required, eg "2024-10-01"')
        if pdate.day is None:
            raise ValueError('A day is required, eg "2024-10-01"')
        request = {
            'variable': [
                '2m_dewpoint_temperature',
                '2m_temperature',
                'skin_temperature',
                'soil_temperature_level_1',
                'soil_temperature_level_2',
                'soil_temperature_level_3',
                'soil_temperature_level_4',
                '10m_u_component_of_wind',
                '10m_v_component_of_wind',
                'forecast_albedo',
                'lake_bottom_temperature',
                'lake_ice_depth',
                'lake_ice_temperature',
                'lake_mix_layer_depth',
                'lake_mix_layer_temperature',
                'lake_shape_factor',
                'lake_total_layer_temperature',
                'leaf_area_index_high_vegetation',
                'leaf_area_index_low_vegetation',
                'skin_reservoir_content',
                'snow_albedo',
                'snow_cover',
                'snow_density',
                'snow_depth',
                'snow_depth_water_equivalent',
                'surface_pressure',
                'temperature_of_snow_layer',
                'volumetric_soil_water_layer_1',
                'volumetric_soil_water_layer_2',
                'volumetric_soil_water_layer_3',
                'volumetric_soil_water_layer_4'
            ],
            'year': pdate.year,
            'month': pdate.month,
            'day': pdate.day,
            'daily_statistic': 'daily_mean',
            'time_zone': 'utc+00:00',
            'frequency': '6_hourly'
        }
        path = source_path(source, f'{dataset}_{str(pdate)}.nc')
        logging.info('creating:%s', path.parent)
        path.parent.mkdir(parents=True, exist_ok=True)

    # Complete ERA5 global atmospheric reanalysis
    # https://cds.climate.copernicus.eu/datasets/reanalysis-era5-complete
    # https://apps.ecmwf.int/codes/grib/param-db/
    if dataset == 'reanalysis-era5-complete':
        request = {
            'date': str(pdate),
            # levelist: 1 is top level, 137 the lowest model level in ERA5. Use
            # '/' to separate values.
            'levelist': '1/10/100/137',
            'levtype': 'ml',
            'param': '130',
            'stream': 'oper',
            'time': '00/to/23/by/6',
            'type': 'an',
            # area: North, West, South, East. Default: global
            'area': '80/-50/-25/0',
            # grid: Latitude/longitude. Default: spherical harmonics or reduced
            # Gaussian grid
            'grid': '1.0/1.0',
            'format': 'netcdf',
        }
        path = source_path(source, f'{dataset}_{str(pdate)}.nc')
        logging.info('creating:%s', path.parent)
        path.parent.mkdir(parents=True, exist_ok=True)

    # ERA5 hourly data on single levels from 1940 to present
    # https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels
    if dataset == 'reanalysis-era5-single-levels':
        if pdate.month is None:
            raise ValueError('A month and a day are required, eg "2024-10-01"')
        if pdate.day is None:
            raise ValueError('A day is required, eg "2024-10-01"')
        request = {
            'product_type': [
                'reanalysis',
                'ensemble_members',
                'ensemble_mean',
                'ensemble_spread'
            ],
            'variable': [
                '10m_u_component_of_wind',
                '10m_v_component_of_wind',
                '2m_dewpoint_temperature',
                '2m_temperature',
                'mean_sea_level_pressure',
                'mean_wave_direction',
                'mean_wave_period',
                'sea_surface_temperature',
                'significant_height_of_combined_wind_waves_and_swell',
                'surface_pressure',
                'total_precipitation',
                '100m_u_component_of_wind',
                '100m_v_component_of_wind',
                '10m_u_component_of_neutral_wind',
                '10m_v_component_of_neutral_wind',
                '10m_wind_gust_since_previous_post_processing',
                'air_density_over_the_oceans',
                'angle_of_sub_gridscale_orography',
                'anisotropy_of_sub_gridscale_orography',
                'benjamin_feir_index',
                'boundary_layer_dissipation',
                'boundary_layer_height',
                'charnock',
                'clear_sky_direct_solar_radiation_at_surface',
                'cloud_base_height',
                'coefficient_of_drag_with_waves',
                'convective_available_potential_energy',
                'convective_inhibition',
                'convective_precipitation',
                'convective_rain_rate',
                'convective_snowfall',
                'convective_snowfall_rate_water_equivalent',
                'downward_uv_radiation_at_the_surface',
                'duct_base_height',
                'eastward_gravity_wave_surface_stress',
                'eastward_turbulent_surface_stress',
                'evaporation',
                'forecast_albedo',
                'forecast_logarithm_of_surface_roughness_for_heat',
                'forecast_surface_roughness',
                'free_convective_velocity_over_the_oceans',
                'friction_velocity',
                'geopotential',
                'gravity_wave_dissipation',
                'high_cloud_cover',
                'high_vegetation_cover',
                'ice_temperature_layer_1',
                'ice_temperature_layer_2',
                'ice_temperature_layer_3',
                'ice_temperature_layer_4',
                'instantaneous_10m_wind_gust',
                'instantaneous_eastward_turbulent_surface_stress',
                'instantaneous_large_scale_surface_precipitation_fraction',
                'instantaneous_moisture_flux',
                'instantaneous_northward_turbulent_surface_stress',
                'instantaneous_surface_sensible_heat_flux',
                'k_index',
                'lake_bottom_temperature',
                'lake_cover',
                'lake_depth',
                'lake_ice_depth',
                'lake_ice_temperature',
                'lake_mix_layer_depth',
                'lake_mix_layer_temperature',
                'lake_shape_factor',
                'lake_total_layer_temperature',
                'land_sea_mask',
                'large_scale_precipitation',
                'large_scale_precipitation_fraction',
                'large_scale_rain_rate',
                'large_scale_snowfall',
                'large_scale_snowfall_rate_water_equivalent',
                'leaf_area_index_high_vegetation',
                'leaf_area_index_low_vegetation',
                'low_cloud_cover',
                'low_vegetation_cover',
                'maximum_2m_temperature_since_previous_post_processing',
                'maximum_individual_wave_height',
                'maximum_total_precipitation_rate_since_previous_' +
                'post_processing',
                'mean_boundary_layer_dissipation',
                'mean_convective_precipitation_rate',
                'mean_convective_snowfall_rate',
                'mean_direction_of_total_swell',
                'mean_direction_of_wind_waves',
                'mean_eastward_gravity_wave_surface_stress',
                'mean_eastward_turbulent_surface_stress',
                'mean_evaporation_rate',
                'mean_gravity_wave_dissipation',
                'mean_large_scale_precipitation_fraction',
                'mean_large_scale_precipitation_rate',
                'mean_large_scale_snowfall_rate',
                'mean_northward_gravity_wave_surface_stress',
                'mean_northward_turbulent_surface_stress',
                'mean_period_of_total_swell',
                'mean_period_of_wind_waves',
                'mean_potential_evaporation_rate',
                'mean_runoff_rate',
                'mean_snow_evaporation_rate',
                'mean_snowfall_rate',
                'mean_snowmelt_rate',
                'mean_square_slope_of_waves',
                'mean_sub_surface_runoff_rate',
                'mean_surface_direct_short_wave_radiation_flux',
                'mean_surface_direct_short_wave_radiation_flux_clear_sky',
                'mean_surface_downward_long_wave_radiation_flux',
                'mean_surface_downward_long_wave_radiation_flux_clear_sky',
                'mean_surface_downward_short_wave_radiation_flux',
                'mean_surface_downward_short_wave_radiation_flux_clear_sky',
                'mean_surface_downward_uv_radiation_flux',
                'mean_surface_latent_heat_flux',
                'mean_surface_net_long_wave_radiation_flux',
                'mean_surface_net_long_wave_radiation_flux_clear_sky',
                'mean_surface_net_short_wave_radiation_flux',
                'mean_surface_net_short_wave_radiation_flux_clear_sky',
                'mean_surface_runoff_rate',
                'mean_surface_sensible_heat_flux',
                'mean_top_downward_short_wave_radiation_flux',
                'mean_top_net_long_wave_radiation_flux',
                'mean_top_net_long_wave_radiation_flux_clear_sky',
                'mean_top_net_short_wave_radiation_flux',
                'mean_top_net_short_wave_radiation_flux_clear_sky',
                'mean_total_precipitation_rate',
                'mean_vertical_gradient_of_refractivity_inside_trapping_layer',
                'mean_vertically_integrated_moisture_divergence',
                'mean_wave_direction_of_first_swell_partition',
                'mean_wave_direction_of_second_swell_partition',
                'mean_wave_direction_of_third_swell_partition',
                'mean_wave_period_based_on_first_moment',
                'mean_wave_period_based_on_first_moment_for_swell',
                'mean_wave_period_based_on_first_moment_for_wind_waves',
                'mean_wave_period_based_on_second_moment_for_swell',
                'mean_wave_period_based_on_second_moment_for_wind_waves',
                'mean_wave_period_of_first_swell_partition',
                'mean_wave_period_of_second_swell_partition',
                'mean_wave_period_of_third_swell_partition',
                'mean_zero_crossing_wave_period',
                'medium_cloud_cover',
                'minimum_2m_temperature_since_previous_post_processing',
                'minimum_total_precipitation_rate_since_previous_' +
                'post_processing',
                'minimum_vertical_gradient_of_refractivity_inside_' +
                'trapping_layer',
                'model_bathymetry',
                'near_ir_albedo_for_diffuse_radiation',
                'near_ir_albedo_for_direct_radiation',
                'normalized_energy_flux_into_ocean',
                'normalized_energy_flux_into_waves',
                'normalized_stress_into_ocean',
                'northward_gravity_wave_surface_stress',
                'northward_turbulent_surface_stress',
                'ocean_surface_stress_equivalent_10m_neutral_wind_direction',
                'ocean_surface_stress_equivalent_10m_neutral_wind_speed',
                'peak_wave_period',
                'period_corresponding_to_maximum_individual_wave_height',
                'potential_evaporation',
                'precipitation_type',
                'runoff',
                'sea_ice_cover',
                'significant_height_of_total_swell',
                'significant_height_of_wind_waves',
                'significant_wave_height_of_first_swell_partition',
                'significant_wave_height_of_second_swell_partition',
                'significant_wave_height_of_third_swell_partition',
                'skin_reservoir_content',
                'skin_temperature',
                'slope_of_sub_gridscale_orography',
                'snow_albedo',
                'snow_density',
                'snow_depth',
                'snow_evaporation',
                'snowfall',
                'snowmelt',
                'soil_temperature_level_1',
                'soil_temperature_level_2',
                'soil_temperature_level_3',
                'soil_temperature_level_4',
                'soil_type',
                'standard_deviation_of_filtered_subgrid_orography',
                'standard_deviation_of_orography',
                'sub_surface_runoff',
                'surface_latent_heat_flux',
                'surface_net_solar_radiation',
                'surface_net_solar_radiation_clear_sky',
                'surface_net_thermal_radiation',
                'surface_net_thermal_radiation_clear_sky',
                'surface_runoff',
                'surface_sensible_heat_flux',
                'surface_solar_radiation_downward_clear_sky',
                'surface_solar_radiation_downwards',
                'surface_thermal_radiation_downward_clear_sky',
                'surface_thermal_radiation_downwards',
                'temperature_of_snow_layer',
                'toa_incident_solar_radiation',
                'top_net_solar_radiation',
                'top_net_solar_radiation_clear_sky',
                'top_net_thermal_radiation',
                'top_net_thermal_radiation_clear_sky',
                'total_cloud_cover',
                'total_column_cloud_ice_water',
                'total_column_cloud_liquid_water',
                'total_column_ozone',
                'total_column_rain_water',
                'total_column_snow_water',
                'total_column_supercooled_liquid_water',
                'total_column_water',
                'total_column_water_vapour',
                'total_sky_direct_solar_radiation_at_surface',
                'total_totals_index',
                'trapping_layer_base_height',
                'trapping_layer_top_height',
                'type_of_high_vegetation',
                'type_of_low_vegetation',
                'u_component_stokes_drift',
                'uv_visible_albedo_for_diffuse_radiation',
                'uv_visible_albedo_for_direct_radiation',
                'v_component_stokes_drift',
                'vertical_integral_of_divergence_of_cloud_frozen_water_flux',
                'vertical_integral_of_divergence_of_cloud_liquid_water_flux',
                'vertical_integral_of_divergence_of_geopotential_flux',
                'vertical_integral_of_divergence_of_kinetic_energy_flux',
                'vertical_integral_of_divergence_of_mass_flux',
                'vertical_integral_of_divergence_of_moisture_flux',
                'vertical_integral_of_divergence_of_ozone_flux',
                'vertical_integral_of_divergence_of_thermal_energy_flux',
                'vertical_integral_of_divergence_of_total_energy_flux',
                'vertical_integral_of_eastward_cloud_frozen_water_flux',
                'vertical_integral_of_eastward_cloud_liquid_water_flux',
                'vertical_integral_of_eastward_geopotential_flux',
                'vertical_integral_of_eastward_heat_flux',
                'vertical_integral_of_eastward_kinetic_energy_flux',
                'vertical_integral_of_eastward_mass_flux',
                'vertical_integral_of_eastward_ozone_flux',
                'vertical_integral_of_eastward_total_energy_flux',
                'vertical_integral_of_eastward_water_vapour_flux',
                'vertical_integral_of_energy_conversion',
                'vertical_integral_of_kinetic_energy',
                'vertical_integral_of_mass_of_atmosphere',
                'vertical_integral_of_mass_tendency',
                'vertical_integral_of_northward_cloud_frozen_water_flux',
                'vertical_integral_of_northward_cloud_liquid_water_flux',
                'vertical_integral_of_northward_geopotential_flux',
                'vertical_integral_of_northward_heat_flux',
                'vertical_integral_of_northward_kinetic_energy_flux',
                'vertical_integral_of_northward_mass_flux',
                'vertical_integral_of_northward_ozone_flux',
                'vertical_integral_of_northward_total_energy_flux',
                'vertical_integral_of_northward_water_vapour_flux',
                'vertical_integral_of_potential_and_internal_energy',
                'vertical_integral_of_potential_internal_and_latent_energy',
                'vertical_integral_of_temperature',
                'vertical_integral_of_thermal_energy',
                'vertical_integral_of_total_energy',
                'vertically_integrated_moisture_divergence',
                'volumetric_soil_water_layer_1',
                'volumetric_soil_water_layer_2',
                'volumetric_soil_water_layer_3',
                'volumetric_soil_water_layer_4',
                'wave_spectral_directional_width',
                'wave_spectral_directional_width_for_swell',
                'wave_spectral_directional_width_for_wind_waves',
                'wave_spectral_kurtosis',
                'wave_spectral_peakedness',
                'wave_spectral_skewness',
                'zero_degree_level'
            ],
            'year': pdate.year,
            'month': pdate.month,
            'day': pdate.day,
            'time': ['12:00'],
            'data_format': 'netcdf',
            'download_format': 'unarchived'
        }
        path = source_path(source, f'{dataset}_{str(pdate)}.nc')
        logging.info('creating:%s', path.parent)
        path.parent.mkdir(parents=True, exist_ok=True)

    client = cdsapi.Client()
    logging.info('exporting:%s', path)
    client.retrieve(dataset, request, path)

    return None
