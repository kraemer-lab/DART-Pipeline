"""
Run unit tests on process_data.py.

Past runs
---------
- 2024-02-08 on Ubuntu 22.04 using Python 3.12: Ran 2 tests in 0.824s
- 2024-02-09 on macOS Sonoma using Python 3.12: Ran 2 tests in 0.833s
- 2024-05-08 on Ubuntu 20.04 using Python 3.12: Ran 18 tests in 7m1.735s
- 2024-05-09 on Ubuntu 22.04 using Python 3.12: Ran 18 tests in 14m58.102s
- 2024-05-10 on macOS Sonoma using Python 3.12: Ran 18 tests in 4m41.547s
- 2024-06-19 on macOS Sonoma using Python 3.12: Ran 22 tests in 4m51.553ss
- 2024-07-04 on Ubuntu 22.04 using Python 3.12: Ran 18 tests in 5m39.803s
- 2024-07-10 on Ubuntu 22.04 using Python 3.12: Ran 20 tests in 5m1.282s
"""
# External libraries
import rasterio
# Built-in modules
from datetime import datetime
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
    process_gadm_worldpopdensity_data, \
    process_economic_geospatial_sociodemographic_data, \
    process_pop_weighted_relative_wealth_index_data
import utils


class TestCases(unittest.TestCase):

    def test_days_to_date(self):
        """No prerequisite data is required to run this test."""
        expected = datetime(1970, 1, 1)
        actual = days_to_date(25567)
        self.assertEqual(expected, actual)

    def test_pixel_to_latlon(self):
        """No prerequisite data is required to run this test."""
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
        """
        Prerequisite data: Relative Wealth Index/VNM.csv'
        Download via: `python3 collate_data.py RWI -3 VNM`
        """
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
        data_name = 'GADM administrative map'
        admin_level = '0'
        iso3 = 'VNM'
        process_geospatial_data(data_name, admin_level, iso3)
        self.test_process_gadm_admin_map_data()

    def test_process_gadm_admin_map_data(self):
        """
        Prerequisite data: gadm41_VNM_0.shp
        Download via: `python3 collate_data.py GADM -1`
        """
        admin_level = '0'
        iso3 = 'VNM'
        process_gadm_admin_map_data(admin_level, iso3)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Geospatial Data',
            'GADM administrative map', 'VNM', 'Admin Level 0', 'Vietnam.png'
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
        process_meteorological_data(data_name, '2023', '11', None, test=True)
        self.test_process_terraclimate_data()

    def test_process_aphrodite_precipitation_data(self):
        """
        Prerequisite data: APHRO_MA_050deg_V1901.2015.gz
        Download via: `python3 collate_data.py "APHRODITE precipitation" -1`
        """
        process_aphrodite_precipitation_data()
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data',
            'APHRODITE Daily accumulated precipitation (V1901)',
            '050deg.csv'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_aphrodite_temperature_data(self):
        """
        Prerequisite data: APHRO_MA_TAVE_050deg_V1808.2015.nc.gz
        Download via: `python3 collate_data.py "APHRODITE temperature" -1`
        """
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
        """
        Prerequisite data: chirps-v2.0.2024.01.01.tif
        Download via: `python3 collate_data.py "CHIRPS rainfall" -1`
        """
        process_chirps_rainfall_data('2024', verbose=False, test=True)
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
        """
        Prerequisite data: ERA5-ml-temperature-subarea.nc
        Download via: `python3 collate_data.py "ERA5 reanalysis" -1`
        """
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
        """
        Prerequisite data: TerraClimate_aet_2023.nc
        Download via: `python3 collate_data.py "TerraClimate data" -1`
        """
        year = '2023'
        month = '11'
        process_terraclimate_data(year, month, verbose=False, test=True)
        base_dir = utils.get_base_directory()

        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data', 'TerraClimate',
            '2023', '11', 'Water Evaporation Amount.png'

        )
        self.assertTrue(path.exists())

        path = Path(
            base_dir, 'B Process Data', 'Meteorological Data', 'TerraClimate',
            'Output.csv'
        )
        self.assertTrue(path.exists())

    def test_process_socio_demographic_data(self):
        data_name = 'Meta population density'
        year = '2020'
        iso3 = 'VNM'
        rt = None
        test = None
        process_socio_demographic_data(data_name, year, iso3, rt, test)
        self.test_process_meta_pop_density_data()

        data_name = 'WorldPop population count'
        process_socio_demographic_data(data_name, '2020', 'VNM', 'ppp', True)
        self.test_process_worldpop_pop_count_data()

        data_name = 'WorldPop population density'
        process_socio_demographic_data(data_name, '2020', 'VNM', 'ppp')
        self.test_process_worldpop_pop_density_data()

    def test_process_meta_pop_density_data(self):
        """
        Prerequisite data: vnm_general_2020.csv
        Download via: `python3 collate_data.py "Meta pop density" -1 -3 VNM`
        """
        year = '2020'
        iso3 = 'VNM'
        process_meta_pop_density_data(year, iso3)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Socio-Demographic Data',
            'Meta population density', 'VNM', '2020',
            'Vietnam.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_worldpop_pop_count_data(self):
        """
        Prerequisite data: VNM_ppp_v2b_2020_UNadj.tif
        Download via: `python3 collate_data.py "WorldPop pop count" -1`
        """
        process_worldpop_pop_count_data('2020', 'VNM', 'ppp', True)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Socio-Demographic Data',
            'WorldPop population count', 'VNM',
            'VNM_ppp_v2b_2020_UNadj - Raw.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_process_worldpop_pop_density_data(self):
        """
        Prerequisite data: vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip
        Download via: `python3 collate_data.py "WorldPop pop density" -1`
        """
        process_worldpop_pop_density_data('2020', 'VNM')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data', 'Socio-Demographic Data',
            'WorldPop population density', 'VNM',
            'vnm_pd_2020_1km_UNadj_ASCII_XYZ.png'
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
        """
        Prerequisite data:
            - gadm41_VNM_shp.zip
            - chirps-v2.0.2024.01.01.tif.gz
        Download via:
            - `python3 collate_data.py GADM -1`
            - `python3 collate_data.py "CHIRPS rainfall" -1`
        """
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
        """
        Prerequisite data:
            - gadm41_VNM_shp.zip
            - VNM_ppp_v2b_2020_UNadj.tif
        Download via:
            - `python3 collate_data.py GADM -1`
            - `python3 collate_data.py "WorldPop pop count" -1`
        """
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
        """
        Prerequisite data:
            - gadm41_VNM_shp.zip
            - vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip
        Download via:
            - `python3 collate_data.py GADM -1`
            - `python3 collate_data.py "WorldPop pop density"`
        """
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

    def test_process_economic_geospatial_sociodemographic_data(self):
        data_name_1 = 'Relative Wealth Index'
        data_name_2 = 'GADM administrative map'
        data_name_3 = 'Meta population density'
        data_name = [data_name_1, data_name_2, data_name_3]
        iso3 = 'VNM'
        admin_level = '0'
        process_economic_geospatial_sociodemographic_data(
            data_name, iso3, admin_level
        )
        self.test_process_pop_weighted_relative_wealth_index_data()

    def test_process_pop_weighted_relative_wealth_index_data(self):
        """
        Prerequisite data:
            - Relative Wealth Index/VNM.csv
            - gadm41_VNM_0.shp
            - vnm_general_2020.csv
        Download via:
            - `python3 collate_data.py RWI -3 VNM`
            - `python3 collate_data.py "Meta pop density" -1 -3 VNM`
            - `python3 collate_data.py GADM -1`
        """
        iso3 = 'VNM'
        admin_level = '0'
        process_pop_weighted_relative_wealth_index_data(iso3, admin_level)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'B Process Data',
            'Economic, Geospatial and Socio-Demographic Data',
            'Relative Wealth Index, GADM administrative map and ' +
            'Meta population density',
            iso3, f'Admin Level {admin_level}.png'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
