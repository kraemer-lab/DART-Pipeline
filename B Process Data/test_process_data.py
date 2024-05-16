"""
Run unit tests on process_data.py.

Past runs
---------
- 2024-02-08 on Ubuntu 22.04 using Python 3.12: Ran 2 tests in 00:00.824
- 2024-02-09 on macOS Sonoma using Python 3.12: Ran 2 tests in 00:00.833
- 2024-05-08 on Ubuntu 20.04 using Python 3.12: Ran 18 tests in 07:01.735
- 2024-05-09 on Ubuntu 22.04 using Python 3.12: Ran 18 tests in 14:58.102
- 2024-05-10 on macOS Sonoma using Python 3.12: Ran 18 tests in 04:41.547
"""
# External libraries
import rasterio
# Built-in modules
from datetime import datetime, timedelta
from pathlib import Path
import os
import time
import unittest
# Custom modules
from process_data import \
    days_to_date, \
    pixel_to_latlon, \
    process_economic_data, \
    process_relative_wealth_index_data, \
    process_epidemiological_data, \
    process_ministerio_de_salud_peru_data, \
    process_geospatial_data, \
    process_gadm_admin_map_data, \
    process_meteorological_data, \
    process_aphrodite_precipitation_data, \
    process_aphrodite_temperature_data, \
    process_chirps_rainfall_data, \
    process_era5_reanalysis_data, \
    process_terraclimate_data, \
    process_socio_demographic_data, \
    process_meta_pop_density_data, \
    process_worldpop_pop_count_data, \
    process_worldpop_pop_density_data, \
    process_geospatial_meteorological_data, \
    process_gadm_chirps_data, \
    process_geospatial_sociodemographic_data, \
    process_gadm_worldpoppopulation_data, \
    process_gadm_worldpopdensity_data,\
    process_economic_geospatial_sociodemographic_data, \
    process_pop_weighted_relative_wealth_index_data
import utils


class TestCases(unittest.TestCase):

    def test_days_to_date(self):
        expected = datetime(1970, 1, 1)
        actual = days_to_date(25567)
        self.assertEqual(expected, actual)

    def test_pixel_to_latlon(self):
        x = [0]
        y = [0]
        transform = [
            102.14, 0.00, 0.00,
            23.39, 0.00, -0.00,
        ]
        affine_transform = rasterio.Affine.from_gdal(*transform)
        lat, lon = pixel_to_latlon(x, y, affine_transform)

        # Perform the test
        expected = [[23.39]], [[102.14]]
        actual = lat, lon
        self.assertEqual(expected, actual)

    def test_process_economic_data(self):
        # Current time
        unix_time = time.time()
        # Process the data
        process_economic_data('Relative Wealth Index', 'VNM')
        # Check the modification time of the output
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Economic Data',
            'Relative Wealth Index', 'VNM.png'
        )
        modification_time = os.path.getmtime(path)
        # Has the output file been modified since this test started running?
        expected = True
        actual = unix_time < modification_time
        self.assertEqual(expected, actual)

    def test_process_relative_wealth_index_data(self):
        # Current time
        unix_time = time.time()
        # Process the data
        process_relative_wealth_index_data(iso3='VNM')
        # Check the modification time of the output
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Economic Data',
            'Relative Wealth Index', 'VNM.png'
        )
        modification_time = os.path.getmtime(path)
        # Has the output file been modified since this test started running?
        expected = True
        actual = unix_time < modification_time
        self.assertEqual(expected, actual)

    def test_process_epidemiological_data(self):
        process_epidemiological_data(
            'Ministerio de Salud (Peru) data', 'PER', '0'
        )
        self.test_process_ministerio_de_salud_peru_data()

    def test_process_ministerio_de_salud_peru_data(self):
        process_ministerio_de_salud_peru_data('0')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Epidemiological Data',
            'Ministerio de Salud (Peru) data', 'Admin Level 0',
            'Admin Level 0.csv'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)
        path = Path(
            base_dir, 'B Process Data', 'Epidemiological Data',
            'Ministerio de Salud (Peru) data', 'Admin Level 0',
            'Peru.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_geospatial_data(self):
        process_geospatial_data('GADM administrative map', '0', 'VNM')
        self.test_process_gadm_admin_map_data()

    def test_process_gadm_admin_map_data(self):
        process_gadm_admin_map_data('0', 'VNM')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Geospatial Data',
            'GADM administrative map', 'VNM', 'gadm41_VNM_shp', 'gadm41_VNM_0',
            'Vietnam.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_meteorological_data(self):
        data_name = 'APHRODITE Daily accumulated precipitation (V1901)'
        process_meteorological_data(data_name, None, None, None)
        self.test_process_aphrodite_precipitation_data()

        data_name = 'APHRODITE Daily mean temperature product (V1808)'
        process_meteorological_data(data_name, None, None, None)
        self.test_process_aphrodite_temperature_data()

        data_name = 'CHIRPS: Rainfall Estimates from Rain Gauge and ' + \
            'Satellite Observations'
        process_meteorological_data(data_name, '2024', None, True)
        self.test_process_chirps_rainfall_data()

        data_name = 'ERA5 atmospheric reanalysis'
        process_meteorological_data(data_name, None, None, None)
        self.test_process_era5_reanalysis_data()

        data_name = 'TerraClimate gridded temperature, precipitation, and ' + \
            'other'
        process_meteorological_data(data_name, '2023', '11', None)
        self.test_process_terraclimate_data()

    def test_process_aphrodite_precipitation_data(self):
        process_aphrodite_precipitation_data()
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'APHRODITE Daily accumulated precipitation (V1901)',
            '025deg.csv'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_aphrodite_temperature_data(self):
        process_aphrodite_temperature_data()
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'APHRODITE Daily mean temperature product (V1808)',
            '005deg.csv'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_chirps_rainfall_data(self):
        process_chirps_rainfall_data('2024', True)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
            'Observations', 'chirps-v2.0.2024.01.01.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_era5_reanalysis_data(self):
        process_era5_reanalysis_data()
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'ERA5 atmospheric reanalysis', 'ERA5-ml-temperature-subarea.csv'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_terraclimate_data(self):
        process_terraclimate_data()
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'TerraClimate', '2023',
            '2023-12-01/water_evaporation_amount_mm.csv'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_socio_demographic_data(self):
        # TODO
        # test_process_meta_pop_density_data()

        data_name = 'WorldPop population count'
        process_socio_demographic_data(data_name, '2020', 'VNM', 'ppp')
        self.test_process_worldpop_pop_count_data()

        data_name = 'WorldPop population density'
        process_socio_demographic_data(data_name, '2020', 'VNM', 'ppp')
        self.test_process_worldpop_pop_density_data()

    # TODO
    # test_process_meta_pop_density_data()

    def test_process_worldpop_pop_count_data(self):
        process_worldpop_pop_count_data('2020', 'VNM', 'ppp')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Socio-Demographic Data',
            'WorldPop population count', 'VNM',
            'VNM_ppp_v2b_2020_UNadj - Log Scale.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_worldpop_pop_density_data(self):
        process_worldpop_pop_density_data('2020', 'VNM')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Socio-Demographic Data',
            'WorldPop population density', '2020', 'VNM',
            'vnm_pd_2020_1km_UNadj_ASCII_XYZ - Naive.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_geospatial_meteorological_data(self):
        data_name = [
            'GADM administrative map',
            'CHIRPS: Rainfall Estimates from Rain Gauge and Satellite ' +
            'Observations'
        ]
        process_geospatial_meteorological_data(data_name, '0', 'VNM', '2024')
        self.test_process_gadm_chirps_data()

    def test_process_gadm_chirps_data(self):
        process_gadm_chirps_data('0', 'VNM', '2024')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Geospatial and Meteorological Data',
            'GADM administrative map and CHIRPS rainfall data', 'VNM',
            'Admin Level 0', 'Rainfall.csv'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)
        path = Path(
            base_dir, 'B Process Data', 'Geospatial and Meteorological Data',
            'GADM administrative map and CHIRPS rainfall data', 'VNM',
            'Admin Level 0', 'Vietnam.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_geospatial_sociodemographic_data(self):
        data_name = [
            'GADM administrative map',
            'WorldPop population count'
        ]
        process_geospatial_sociodemographic_data(
            data_name, '0', 'VNM', '2020', 'ppp'
        )
        self.test_process_gadm_worldpoppopulation_data()

        data_name = [
            'GADM administrative map',
            'WorldPop population density'
        ]
        process_geospatial_sociodemographic_data(
            data_name, '0', 'VNM', '2020', 'ppp'
        )
        self.test_process_gadm_worldpopdensity_data()

    def test_process_gadm_worldpoppopulation_data(self):
        process_gadm_worldpoppopulation_data('0', 'VNM', '2020', 'ppp')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data',
            'Geospatial and Socio-Demographic Data',
            'GADM administrative map and WorldPop population count', 'VNM',
            'Admin Level 0', 'Population.csv'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)
        path = Path(
            base_dir, 'B Process Data',
            'Geospatial and Socio-Demographic Data',
            'GADM administrative map and WorldPop population count', 'VNM',
            'Admin Level 0', 'Vietnam.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_gadm_worldpopdensity_data(self):
        process_gadm_worldpopdensity_data('0', 'VNM', '2020', 'ppp')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data',
            'Geospatial and Socio-Demographic Data',
            'GADM administrative map and WorldPop population density',
            'VNM', 'Admin Level 0', 'Vietnam.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    # TODO
    # test_process_economic_geospatial_sociodemographic_data()

    # TODO
    # test_process_pop_weighted_relative_wealth_index_data()


if __name__ == '__main__':
    unittest.main()
