"""
Run unit tests on collate_data.py.

Past Runs
---------
- 2024-01-15 on Ubuntu 22.04 using Python 3.12: Ran 5 tests in 13.366s
- 2024-01-17 on macOS Sonoma using Python 3.12: Ran 5 tests in 4.376s
- 2024-02-08 on Ubuntu 22.04 using Python 3.12: Ran 4 tests in 10.968s
- 2024-02-09 on macOS Sonoma using Python 3.12: Ran 4 tests in 6.190s
- 2024-02-09 on macOS Sonoma using Python 3.12: Ran 4 tests in 6.190s
- 2024-02-14 on macOS Sonoma using Python 3.12: Ran 5 tests in 4.516s
"""
import unittest
from unittest.mock import patch
from pathlib import Path
# Custom modules
from collate_data import \
    get_credentials, \
    walk, \
    download_gadm_data, \
    download_file, \
    unpack_file


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

        # Call the function being tested
        username, password = get_credentials('Example metric')

        # Perform the tests
        self.assertEqual(username, 'Example username')
        self.assertEqual(password, 'Example password')

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
        level = 'level0'
        download_gadm_data(file_format, out_dir, iso3, level)

        # Check if the file has been created
        expected = True
        path = Path('tests/gadm41_VNM_0.json')
        actual = path.exists()

        # Perform the test
        self.assertEqual(expected, actual)

        # Tear down
        path.unlink()
        Path('tests/').rmdir()

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


if __name__ == '__main__':
    unittest.main()
