"""
Run unit tests on utils.py.

Past Runs
---------
None
"""
import unittest
from unittest.mock import patch
import utils
import sys
import os
from pathlib import Path
import warnings

path = os.path.join(utils.get_base_directory(), 'A Collate Data')


class TestCases(unittest.TestCase):

    def test_get_base_directory(self):
        expected = str(Path('~/DART-Pipeline').expanduser())
        actual = utils.get_base_directory()
        self.assertEqual(expected, actual)

    @patch('platform.system', return_value='Linux')
    @patch('distro.name', return_value='Mint')
    def test_check_os_mint(self, mock_system, mock_name):
        utils.check_os()

    @patch('platform.system', return_value='Linux')
    @patch('distro.name', return_value='Ubuntu')
    @patch('distro.version', return_value='20.04')
    def test_check_os_ubuntu(self, mock_system, mock_name, mock_version):
        utils.check_os()

    @patch('platform.system', return_value='Windows')
    def test_check_os_windows(self, mock_system):
        utils.check_os()

    @patch('platform.system', return_value='Darwin')
    @patch('platform.mac_ver', return_value=['10.14'])
    def test_check_os_apple(self, mock_system, mock_mac_ver):
        utils.check_os()

    @patch('platform.system', return_value='ArcaOS')
    def test_check_os_arca(self, mock_system):
        utils.check_os()

    @patch('platform.python_version', return_value='2.7')
    def test_check_python_2(self, mock_python_version):
        utils.check_python()

    @patch('platform.python_version', return_value='3.6')
    def test_check_python_36(self, mock_python_version):
        utils.check_python()

    @patch('platform.python_version', return_value='3.11')
    def test_check_python_311(self, mock_python_version):
        utils.check_python()

    @patch('platform.python_version', return_value='4.0')
    def test_check_python_4(self, mock_python_version):
        utils.check_python()

    @patch('sys.prefix', sys.base_prefix)
    def test_check_environment_novenv(self):
        with self.assertWarns(UserWarning):
            utils.check_environment()

    @patch('sys.prefix', path)
    def test_check_environment_venv(self):
        with warnings.catch_warnings(record=True) as w:
            utils.check_environment()
            # Assert that there were 0 warnings
            self.assertEqual(len(w), 0)


if __name__ == '__main__':
    unittest.main()
