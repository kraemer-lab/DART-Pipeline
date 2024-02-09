"""
Run unit tests on utils.py.

Past Runs
---------
None
"""
import unittest
from unittest.mock import patch, mock_open
import utils
import sys
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

    @patch('sys.version_info', (4, 0))
    def test_check_python_4(self):
        with self.assertWarns(UserWarning):
            utils.check_python()

    #
    # Test check_environment()
    #

    @patch('sys.prefix', sys.base_prefix)
    @patch('os.path.isfile', return_value=False)
    def test_check_environment_using_base(self, mock_isfile):
        """Test when using base Python."""
        with self.assertWarns(UserWarning):
            utils.check_environment()

    base_dir = str(Path('~/DART-Pipeline').expanduser())
    base_dir_a = Path(base_dir, 'A Collate Data', 'venv')

    @patch('sys.prefix', base_dir_a)
    def test_check_environment_using_venv(self):
        """Test when using a virtual environment."""
        with warnings.catch_warnings(record=True) as w:
            utils.check_environment()
            # Assert that there were 0 warnings
            self.assertEqual(len(w), 0)

    def test_check_environment_using_docker(self):
        """Test when using Docker."""
        contents = '1:name=systemd:/docker-ee/foobar456'
        with patch('builtins.open', mock_open(read_data=contents)):
            with warnings.catch_warnings(record=True) as w:
                utils.check_environment()
                # Assert that there were 0 warnings
                self.assertEqual(len(w), 0)


if __name__ == '__main__':
    unittest.main()
