"""
Run unit tests on collate_data.py.

Past Runs
---------
- 2024-01-15 on Ubuntu 22.04 using Python 3.12: Ran 5 tests in 13.366s
- 2024-01-17 on macOS Sonoma using Python 3.12: Ran 5 tests in 4.376s
- 2024-02-08 on Ubuntu 22.04 using Python 3.12: Ran 4 tests in 10.968s
- 2024-02-09 on macOS Sonoma using Python 3.12: Ran 4 tests in 6.190s
- 2024-02-13 on Ubuntu 22.04 using Python 3.12: Ran 16 tests in 64.283s
"""
import unittest
from collate_data import \
    walk, \
    download_gadm_data, \
    download_file, \
    unpack_file, \
    download_meteorological_data, \
    download_aphrodite_temperature_data, \
    download_aphrodite_precipitation_data, \
    download_chirps_rainfall_data, \
    download_terraclimate_data, \
    download_era5_reanalysis_data, \
    download_socio_demographic_data, \
    download_worldpop_pop_density_data, \
    download_worldpop_pop_count_data, \
    download_geospatial_data, \
    download_gadm_admin_map_data
from pathlib import Path
import utils


class TestCases(unittest.TestCase):

    # TODO
    # def test_get_password(self):

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

    def test_download_file_successful(self):
        """Test a successful download."""
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

    def test_download_file_unsuccessful(self):
        """Test an unsuccessful download."""
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

    def test_download_meteorological_data(self):
        data_name = 'APHRODITE Daily mean temperature product (V1808)'
        download_meteorological_data(data_name, only_one=True, dry_run=True)
        self.test_download_aphrodite_temperature_data()

        data_name = 'APHRODITE Daily accumulated precipitation (V1901)'
        download_meteorological_data(data_name, only_one=True, dry_run=True)
        self.test_download_aphrodite_precipitation_data()

        data_name = 'CHIRPS: Rainfall Estimates from Rain Gauge and ' + \
            'Satellite Observations'
        download_meteorological_data(data_name, only_one=True, dry_run=True)
        self.test_download_chirps_rainfall_data()

        data_name = 'TerraClimate gridded temperature, precipitation, and ' + \
            'other'
        download_meteorological_data(data_name, only_one=True, dry_run=True)
        self.test_download_terraclimate_data()

        data_name = 'ERA5 atmospheric reanalysis'
        download_meteorological_data(data_name, only_one=True, dry_run=True)
        self.test_download_era5_reanalysis_data()

    def test_download_aphrodite_temperature_data(self):
        download_aphrodite_temperature_data(only_one=True, dry_run=True)
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'APHRODITE Daily mean temperature product (V1808)',
            'product', 'APHRO_V1808_TEMP', 'APHRO_MA'
        )
        for branch in [
            Path('005deg', 'APHRO_MA_TAVE_CLM_005deg_V1808.ctl.gz'),
            Path('005deg_nc', 'APHRO_MA_TAVE_CLM_005deg_V1808.nc.gz'),
            Path('025deg', 'APHRO_MA_TAVE_025deg_V1808.1961.gz'),
            Path('025deg_nc', 'APHRO_MA_TAVE_025deg_V1808.1961.nc.gz'),
            Path('050deg', 'read_aphro_v1808.f90'),
            Path('050deg_nc', 'APHRO_MA_TAVE_050deg_V1808.1961.nc.gz'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)

    def test_download_aphrodite_precipitation_data(self):
        download_aphrodite_precipitation_data(only_one=True, dry_run=True)
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'APHRODITE Daily accumulated precipitation (V1901)',
            'product', 'APHRO_V1901', 'APHRO_MA'
        )
        for branch in [
            Path('005deg', 'APHRO_MA_PREC_CLM_005deg_V1901.ctl.gz'),
            Path('025deg', 'APHRO_MA_025deg_V1901.1998.gz'),
            Path('025deg_nc', 'APHRO_MA_025deg_V1901.1998.nc.gz'),
            Path('050deg', 'APHRO_MA_050deg_V1901.1998.gz'),
            Path('050deg_nc', 'APHRO_MA_050deg_V1901.1998.nc.gz'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)

    def test_download_chirps_rainfall_data(self):
        download_chirps_rainfall_data(only_one=True, dry_run=True)
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'CHIRPS - Rainfall Estimates from Rain Gauge and Satellite ' +
            'Observations', 'products', 'CHIRPS-2.0', 'global_daily', 'tifs',
            'p05'
        )
        for branch in [
            Path('2023', 'chirps-v2.0.2023.01.01.tif.gz'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)

    def test_download_terraclimate_data(self):
        download_terraclimate_data(only_one=True, dry_run=True)
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'TerraClimate gridded temperature, precipitation, and other',
            'TERRACLIMATE-DATA'
        )
        for branch in [
            Path('fix_unsigned.sh'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)

    def test_download_era5_reanalysis_data(self):
        download_era5_reanalysis_data(only_one=True, dry_run=True)
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Meteorological Data',
            'ERA5 atmospheric reanalysis',
        )
        for branch in [
            Path('ERA5-ml-temperature-subarea.nc'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)

    def test_download_socio_demographic_data(self):
        data_name = 'WorldPop population density'
        download_socio_demographic_data(data_name, False, True)
        self.test_download_worldpop_pop_density_data()

        data_name = 'WorldPop population count'
        download_socio_demographic_data(data_name, False, True)
        self.test_download_worldpop_pop_count_data()

    def test_download_worldpop_pop_density_data(self):
        download_worldpop_pop_density_data(only_one=False, dry_run=True)
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Socio-Demographic Data',
            'WorldPop population density', 'GIS', 'Population_Density',
            'Global_2000_2020_1km_UNadj', '2020', 'VNM'
        )
        for branch in [
            Path('vnm_pd_2020_1km_UNadj_ASCII_XYZ.zip'),
            Path('vnm_pd_2020_1km_UNadj.tif'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)

    def test_download_worldpop_pop_count_data(self):
        download_worldpop_pop_count_data(only_one=False, dry_run=True)
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Socio-Demographic Data',
            'WorldPop population count', 'GIS', 'Population',
            'Individual_countries', 'VNM'
        )
        for branch in [
            Path('Viet_Nam_100m_Population.7z'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)

    def test_download_geospatial_data(self):
        data_name = 'GADM administrative map'
        download_geospatial_data(data_name, only_one=False, dry_run=True)
        self.test_download_gadm_admin_map_data()

    def test_download_gadm_admin_map_data(self):
        download_gadm_admin_map_data(only_one=False, dry_run=True)
        base_dir = utils.get_base_directory()
        root = Path(
            base_dir, 'A Collate Data', 'Geospatial Data',
            'GADM administrative map',
        )
        for branch in [
            Path('gadm41_VNM.gpkg'),
            Path('gadm41_VNM_shp.zip'),
            Path('gadm41_VNM_0.json'),
            Path('gadm41_VNM_1.json.zip'),
            Path('gadm41_VNM_2.json.zip'),
            Path('gadm41_VNM_3.json.zip'),
        ]:
            expected = True
            actual = Path(root, branch).exists()
            self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
