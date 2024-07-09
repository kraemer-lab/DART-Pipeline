"""
Run unit tests on collate_data.py.

Past Runs
---------
- 2024-01-15 on Ubuntu 22.04 using Python 3.12: Ran 5 tests in 13.366s
- 2024-01-17 on macOS Sonoma using Python 3.12: Ran 5 tests in 4.376s
- 2024-02-08 on Ubuntu 22.04 using Python 3.12: Ran 4 tests in 10.968s
- 2024-02-09 on macOS Sonoma using Python 3.12: Ran 4 tests in 6.190s
- 2024-02-13 on Ubuntu 22.04 using Python 3.12: Ran 16 tests in 1m4.283s
- 2024-02-14 on macOS Sonoma using Python 3.12:
    - Ran 18 tests in 42.751s
    - Ran 18 tests in 41.872s
- 2024-04-23 on Ubuntu 22.04 using Python 3.12: Ran 17 tests in 31.534s
- 2024-05-08:
    - On Ubuntu 22.04 using Python 3.12: Ran 17 tests in 1m00.448s
    - On Ubuntu 20.04 using Python 3.12: Ran 17 tests in 21.853s
- 2024-05-10: on macOS Sonoma using Python 3.12:
    - Ran 17 tests in 37.624s
    - Ran 19 tests in 5m37.731s
- 2024-06-06: on Ubuntu 22.04 using Python 3.12: Ran 19 tests in 5m4.979s
"""
from pathlib import Path
from unittest.mock import patch
import os
import unittest
# Custom modules
import utils
from collate_data import \
    get_credentials, \
    download_file, \
    download_files, \
    walk, \
    download_gadm_data, \
    unpack_file, \
    download_epidemiological_data, \
    download_ministerio_de_salud_peru_data, \
    download_geospatial_data, \
    download_gadm_admin_map_data, \
    download_meteorological_data, \
    download_aphrodite_temperature_data, \
    download_aphrodite_precipitation_data, \
    download_chirps_rainfall_data, \
    download_era5_reanalysis_data, \
    download_terraclimate_data, \
    download_socio_demographic_data, \
    download_worldpop_pop_count_data, \
    download_worldpop_pop_density_data


class TestCases(unittest.TestCase):

    @patch('collate_data.open')
    def test_get_credentials(self, mock_open):
        # Mock the contents of the credentials file
        file = '''{
            "Example metric": {
                "username": "Example username",
                "password": "Example password"
            }
        }'''
        mock_open.return_value.__enter__.return_value.read.return_value = file

        # Test the functionality when the file is in the default location
        username, password = get_credentials('Example metric')
        self.assertEqual(username, 'Example username')
        self.assertEqual(password, 'Example password')

        # Test the functionality when the file is in an unexpected location
        path = Path('alternative/path/to/example_credentials.json')
        uname, password = get_credentials('Example metric', credentials=path)
        self.assertEqual(uname, 'Example username')
        self.assertEqual(password, 'Example password')

    def test_download_file(self):
        #
        # Test a successful download
        #
        url = 'https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_VNM_0.json'
        out_dir = Path('tests')
        out_dir.mkdir(parents=True, exist_ok=True)
        path = Path(out_dir, 'gadm41_VNM_0.json')
        succeded = download_file(url, path)
        # Check if the file has been created
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)
        # Check if the value returned from the function is correct
        expected = True
        actual = succeded
        self.assertEqual(expected, actual)
        # Tear down
        path.unlink()
        Path('tests/').rmdir()

        #
        # Test an unsuccessful download
        #
        # This will fail because a password is needed
        url = 'http://aphrodite.st.hirosaki-u.ac.jp/product/' + \
            'APHRO_V1808_TEMP/APHRO_MA/005deg/' + \
            'APHRO_MA_TAVE_CLM_005deg_V1808.ctl.gz'
        out_dir = Path('tests/product/APHRO_V1808_TEMP/APHRO_MA/005deg')
        out_dir.mkdir(parents=True, exist_ok=True)
        path = Path(out_dir, 'APHRO_MA_TAVE_CLM_005deg_V1808.ctl.gz')
        succeded = download_file(url, path)
        # Check if the file has been created
        expected = False
        actual = path.exists()
        self.assertEqual(expected, actual)
        # Check if the value returned from the function is correct
        expected = False
        actual = succeded
        self.assertEqual(expected, actual)
        # Tear down
        Path('tests/product/APHRO_V1808_TEMP/APHRO_MA/005deg').rmdir()
        Path('tests/product/APHRO_V1808_TEMP/APHRO_MA').rmdir()
        Path('tests/product/APHRO_V1808_TEMP').rmdir()
        Path('tests/product').rmdir()
        Path('tests/').rmdir()

    def test_download_files(self):
        # Setup
        out_dir = Path('tests')
        out_dir.mkdir(parents=True, exist_ok=True)

        #
        # Test successful downloads
        #
        base_url = 'https://geodata.ucdavis.edu'
        relative_url = 'gadm/gadm4.1/json'
        files = ['gadm41_VNM_0.json', 'gadm41_VNM_1.json']
        successes = download_files(
            base_url, relative_url, files, only_one=False, dry_run=False,
            out_dir=out_dir
        )
        # Check if the value returned from the function is correct
        expected = [True, True]
        actual = successes
        self.assertEqual(expected, actual)
        # Check if the files exist
        expected = True
        for file in files:
            path = Path(out_dir, relative_url, file)
            actual = path.exists()
            self.assertEqual(expected, actual)
            # Tear down
            path.unlink()
        # Tear down
        Path('tests/gadm/gadm4.1/json/').rmdir()
        Path('tests/gadm/gadm4.1/').rmdir()
        Path('tests/gadm/').rmdir()

        #
        # Test an unsuccessful download
        #
        base_url = 'https://geodata.ucdavis.edu'
        relative_url = 'gadm/gadm4.1/json'
        files = ['gadm41_VNM_0.json', 'gadm41_VNM_4.json']
        successes = download_files(
            base_url, relative_url, files, only_one=False, dry_run=False,
            out_dir=out_dir
        )
        # Check if the value returned from the function is correct
        expected = [True, False]
        actual = successes
        self.assertEqual(expected, actual)
        # Check if the files exist/do not exist
        expected = [True, False]
        for i, file in enumerate(files):
            path = Path(out_dir, relative_url, file)
            actual = path.exists()
            self.assertEqual(expected[i], actual)
        # Tear down
        Path('tests/gadm/gadm4.1/json/gadm41_VNM_0.json').unlink()
        Path('tests/gadm/gadm4.1/json/').rmdir()
        Path('tests/gadm/gadm4.1/').rmdir()
        Path('tests/gadm/').rmdir()
        Path('tests/').rmdir()

    def test_walk(self):
        base_url = 'https://data.chc.ucsb.edu'
        relative_url = 'products/CHIRPS-2.0/docs'
        only_one = True
        dry_run = True
        out_dir = Path('tests')
        out_dir.mkdir(parents=True, exist_ok=True)
        walk(base_url, relative_url, only_one, dry_run, out_dir)

        # Check if the file has been created
        expected = True
        path = Path('tests/products/CHIRPS-2.0/docs/README-CHIRPS.txt')
        actual = path.exists()

        # Perform the test
        self.assertEqual(expected, actual)

        # Tear down
        path.unlink()
        Path('tests/products/CHIRPS-2.0/docs/').rmdir()
        Path('tests/products/CHIRPS-2.0/').rmdir()
        Path('tests/products/').rmdir()
        Path('tests/').rmdir()

    def test_download_gadm_data(self):
        file_format = 'GeoJSON'
        out_dir = Path('tests')
        out_dir.mkdir(parents=True, exist_ok=True)
        iso3 = 'VNM'
        dry_run = True
        level = 'level0'
        download_gadm_data(file_format, out_dir, iso3, dry_run, level)

        # Check if the file has been created
        expected = True
        path = Path('tests/gadm41_VNM_0.json')
        actual = path.exists()

        # Perform the test
        self.assertEqual(expected, actual)

        # Tear down
        path.unlink()
        Path('tests/').rmdir()

    def test_unpack_file(self):
        url = 'https://geodata.ucdavis.edu/gadm/gadm4.1/json/' + \
            'gadm41_VNM_1.json.zip'
        out_dir = Path('tests')
        out_dir.mkdir(parents=True, exist_ok=True)
        path = Path(out_dir, 'gadm41_VNM_1.json.zip')
        succeded = download_file(url, path)
        if succeded:
            unpack_file(path, same_folder=True)

        # Check if the file has been created
        expected = True
        actual = Path('tests/gadm41_VNM_1.json').exists()

        # Perform the test
        self.assertEqual(expected, actual)

        # Tear down
        path.unlink()
        Path(out_dir, 'gadm41_VNM_1.json').unlink()
        Path('tests/').rmdir()

    def test_download_epidemiological_data(self):
        data_name = 'Ministerio de Salud (Peru) data'
        download_epidemiological_data(data_name, True, True, None, None)
        self.test_download_ministerio_de_salud_peru_data()

    def test_download_ministerio_de_salud_peru_data(self):
        download_ministerio_de_salud_peru_data(True, True)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'A Collate Data', 'Epidemiological Data',
            'Ministerio de Salud (Peru) data', 'casos_dengue_AMAZONAS.xlsx'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_download_geospatial_data(self):
        data_name = 'GADM administrative map'
        download_geospatial_data(data_name, False, True, 'VNM')
        self.test_download_gadm_admin_map_data()

    def test_download_gadm_admin_map_data(self):
        download_gadm_admin_map_data(False, True, 'VNM')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'A Collate Data', 'Geospatial Data',
            'GADM administrative map', 'VNM'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_download_meteorological_data(self):
        only_one = True
        dry_run = True

        data_name = 'APHRODITE Daily accumulated precipitation (V1901)'
        download_meteorological_data(data_name, only_one, dry_run)
        self.test_download_aphrodite_precipitation_data()

        data_name = 'APHRODITE Daily mean temperature product (V1808)'
        download_meteorological_data(data_name, only_one, dry_run)
        self.test_download_aphrodite_temperature_data()

        data_name = 'CHIRPS: Rainfall Estimates from Rain Gauge and ' + \
            'Satellite Observations'
        download_meteorological_data(data_name, only_one, dry_run)
        self.test_download_chirps_rainfall_data()

        data_name = 'TerraClimate gridded temperature, precipitation, and ' + \
            'other'
        download_meteorological_data(data_name, only_one, dry_run)
        self.test_download_terraclimate_data()

        data_name = 'ERA5 atmospheric reanalysis'
        download_meteorological_data(data_name, only_one, dry_run)
        self.test_download_era5_reanalysis_data()

    def test_download_aphrodite_precipitation_data(self):
        """
        Using an environment variable to store the credentials:

        $ export CREDENTIALS_JSON='{
            "APHRODITE Daily accumulated precipitation (V1901)": {
                "username": "example@email.com",
                "password": "*******"
            }
        }'
        $ python3 test_collate_data.py
        """
        if 'CREDENTIALS_JSON' in os.environ:
            # If running via GitHub Actions
            download_aphrodite_precipitation_data(True, True, 'environ')
        else:
            # If running directly
            download_aphrodite_precipitation_data(True, True, None)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'APHRODITE Daily accumulated precipitation (V1901)',
            'product', 'APHRO_V1901', 'APHRO_MA', '050deg',
            'APHRO_MA_050deg_V1901.2015.gz'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_download_aphrodite_temperature_data(self):
        """
        Using an environment variable to store the credentials:

        $ export CREDENTIALS_JSON='{
            "APHRODITE Daily mean temperature product (V1808)": {
                "username": "example@email.com",
                "password": "*******"
            }
        }'
        $ python3 test_collate_data.py
        """
        if 'CREDENTIALS_JSON' in os.environ:
            # If running via GitHub Actions
            download_aphrodite_temperature_data(True, True, 'environ')
        else:
            # If running directly
            download_aphrodite_temperature_data(True, True, None)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'APHRODITE Daily mean temperature product (V1808)',
            'product', 'APHRO_V1808_TEMP', 'APHRO_MA', '050deg_nc',
            'APHRO_MA_TAVE_050deg_V1808.2015.nc.gz'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_download_chirps_rainfall_data(self):
        download_chirps_rainfall_data(only_one=True, dry_run=False)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
            'Observations', 'products', 'CHIRPS-2.0', 'global_daily', 'tifs',
            'p05', '2024', 'chirps-v2.0.2024.01.01.tif.gz'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_download_era5_reanalysis_data(self):
        download_era5_reanalysis_data(only_one=True, dry_run=False)
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'ERA5 atmospheric reanalysis', 'ERA5-ml-temperature-subarea.nc'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_download_terraclimate_data(self):
        download_terraclimate_data(only_one=True, dry_run=False, year='2023')
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'TerraClimate gridded temperature, precipitation, and other',
            'TERRACLIMATE-DATA'
        )
        for branch in [
            Path('TerraClimate_aet_2023.nc'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)

    def test_download_socio_demographic_data(self):
        data_name = 'WorldPop population count'
        download_socio_demographic_data(data_name, True, False, 'VNM')
        self.test_download_worldpop_pop_count_data()

        data = 'WorldPop population density'
        data_name = 'WorldPop population density'
        download_socio_demographic_data(data_name, False, True, 'VNM')
        self.test_download_worldpop_pop_density_data()

    def test_download_worldpop_pop_count_data(self):
        download_worldpop_pop_count_data(True, False, 'VNM')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'A Collate Data', 'Socio-Demographic Data',
            'WorldPop population count', 'GIS', 'Population',
            'Individual_countries', 'VNM', 'Viet_Nam_100m_Population',
            'VNM_ppp_v2b_2020_UNadj.tif'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)

    def test_download_worldpop_pop_density_data(self):
        download_worldpop_pop_density_data(False, True, 'VNM')
        base_dir = utils.get_base_directory()
        path = Path(
            base_dir, 'A Collate Data', 'Socio-Demographic Data',
            'WorldPop population density', 'GIS', 'Population_Density',
            'Global_2000_2020_1km_UNadj', '2020', 'VNM',
            'vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip'
        )
        expected = True
        actual = path.exists()
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
