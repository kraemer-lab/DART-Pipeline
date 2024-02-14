"""
Run unit tests on utils.py.

Past Runs
---------
- 2024-02-09 on macOS Sonoma with Python 3.12: Ran 17 tests in 0.010s
- 2024-02-12 on Ubuntu 22 with Python 3.12: Ran 15 tests in 0.050s
"""
import unittest
from unittest.mock import patch
import utils
from pathlib import Path
import warnings


class TestCases(unittest.TestCase):

    #
    # Test get_base_directory()
    #

    def test_get_base_directory_cwd(self):
        """
        Test the current working directory.

        This tests the default behaviour.
        """
        expected = str(Path('~/DART-Pipeline').expanduser())
        actual = utils.get_base_directory()
        self.assertEqual(expected, actual)

    def test_get_base_directory_parent(self):
        """
        Test the parent directory.

        This tests a directory in a Git project that is not the default
        input.
        """
        expected = str(Path('~/DART-Pipeline').expanduser())
        actual = utils.get_base_directory('..')
        self.assertEqual(expected, actual)

    def test_get_base_directory_grandparent(self):
        """
        Test the grandparent directory.

        - This tests an input which is a Path object, not a string.
        - This tests a directory that is not in a Git project.
        """
        expected = None
        actual = utils.get_base_directory(Path('..', '..'))
        self.assertEqual(expected, actual)

    #
    # Test check_os()
    #

    @patch('platform.system', return_value='Linux')
    @patch('distro.name', return_value='Mint')
    def test_check_os_mint(self, mock_system, mock_name):
        with self.assertWarns(UserWarning):
            utils.check_os()

    @patch('platform.system', return_value='Linux')
    @patch('distro.name', return_value='Ubuntu')
    @patch('distro.version', return_value='20.04')
    def test_check_os_ubuntu20(self, mock_system, mock_name, mock_version):
        with self.assertWarns(UserWarning):
            utils.check_os()

    @patch('platform.system', return_value='Linux')
    @patch('distro.name', return_value='Ubuntu')
    @patch('distro.version', return_value='22.04')
    def test_check_os_ubuntu22(self, mock_system, mock_name, mock_version):
        with warnings.catch_warnings(record=True) as w:
            utils.check_os()
            # Assert that there were 0 warnings
            self.assertEqual(len(w), 0)

    @patch('platform.system', return_value='Windows')
    def test_check_os_windows(self, mock_system):
        with self.assertWarns(UserWarning):
            utils.check_os()

    @patch('platform.system', return_value='Darwin')
    @patch('platform.mac_ver', return_value=['10.14'])
    def test_check_os_macos10(self, mock_system, mock_mac_ver):
        with self.assertWarns(UserWarning):
            utils.check_os()

    @patch('platform.system', return_value='Darwin')
    @patch('platform.mac_ver', return_value=['14.01'])
    def test_check_os_macos14(self, mock_system, mock_mac_ver):
        with warnings.catch_warnings(record=True) as w:
            utils.check_os()
            # Assert that there were 0 warnings
            self.assertEqual(len(w), 0)

    @patch('platform.system', return_value='ArcaOS')
    def test_check_os_arca(self, mock_system):
        with self.assertWarns(UserWarning):
            utils.check_os()

    #
    # Test check_python()
    #

    @patch('sys.version_info', (2, 7))
    def test_check_python_2(self):
        with self.assertWarns(UserWarning):
            utils.check_python()

    @patch('sys.version_info', (3, 6))
    def test_check_python_36(self):
        with self.assertWarns(UserWarning):
            utils.check_python()

    @patch('sys.version_info', (3, 11))
    def test_check_python_311(self):
        with self.assertWarns(UserWarning):
            utils.check_python()

    @patch('sys.version_info', (3, 12))
    def test_check_python_312(self):
        with warnings.catch_warnings(record=True) as w:
            utils.check_python()
            # Assert that there were 0 warnings
            self.assertEqual(len(w), 0)

    @patch('sys.version_info', (4, 0))
    def test_check_python_4(self):
        with self.assertWarns(UserWarning):
            utils.check_python()


if __name__ == '__main__':
    unittest.main()