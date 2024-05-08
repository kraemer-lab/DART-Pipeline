"""
Run unit tests on process_data.py.

Past runs
---------
- 2024-02-08 on Ubuntu 22.04 using Python 3.12: Ran 2 tests in 0.824s
- 2024-02-09 on macOS Sonoma using Python 3.12: Ran 2 tests in 0.833s
"""
# External libraries
import rasterio
# Built-in modules
from datetime import datetime
import unittest
from pathlib import Path
# Custom modules
from process_data import \
    days_to_date, \
    pixel_to_latlon, \
    process_geospatial_data, \
    process_gadm_admin_map_data, \
    process_meteorological_data, \
    process_aphrodite_precipitation_data, \
    process_aphrodite_temperature_data, \
    process_chirps_rainfall_data, \
    process_era5_reanalysis_data, \
    process_terraclimate_data, \
    process_socio_demographic_data, \
    process_worldpop_pop_count_data, \
    process_worldpop_pop_density_data, \
    process_geospatial_meteorological_data, \
    process_gadm_chirps_data, \
    process_geospatial_sociodemographic_data, \
    process_gadm_worldpoppopulation_data, \
    process_gadm_worldpopdensity_data
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
        process_terraclimate_data('2023', '11')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'TerraClimate', '2023-11',
            'Water Potential Evaporation Amount.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_socio_demographic_data(self):
        data_name = 'WorldPop population count'
        process_socio_demographic_data(data_name, '2020', 'VNM', 'ppp')
        self.test_process_worldpop_pop_count_data()

        data_name = 'WorldPop population density'
        process_socio_demographic_data(data_name, '2020', 'VNM', 'ppp')
        self.test_process_worldpop_pop_density_data()

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
            'vnm_pd_2020_1km_UNadj_ASCII_XYZ - Log Transformed.png'
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


if __name__ == '__main__':
    unittest.main()
