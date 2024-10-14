"""
Tests for utility functions
"""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import requests_mock
import pandas as pd

from dart_pipeline.util import download_file, days_in_year, get_country_name, \
    use_range, update_or_create_output


def test_download_file():
    url = "http://example.com/a.csv"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "delete_this.csv"
    with requests_mock.Mocker() as m:
        m.get("http://example.com/a.csv", text="colA,colB\n1,2")
        download_file(url, path)
        assert path.read_text() == "colA,colB\n1,2"
        path.unlink()


def test_download_file_unzip():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path)
        # download_file should unpack zip files by default
        assert (source_path / "hello.txt").read_text() == "Hello there\n"
        assert (source_path / "world.txt").read_text() == "Hello world\n"
        path.unlink()
        (source_path / "hello.txt").unlink()
        (source_path / "world.txt").unlink()


def test_download_file_without_unzip():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path, unpack=False)
        assert not (source_path / "hello.txt").exists()
        assert path.exists()
        path.unlink()


def test_download_file_unzip_create_folder():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path, unpack=True, unpack_create_folder=True)
        assert (source_path / "file" / "hello.txt").read_text() == "Hello there\n"
        assert (source_path / "file" / "world.txt").read_text() == "Hello world\n"
        (source_path / "file" / "hello.txt").unlink()
        (source_path / "file" / "world.txt").unlink()


@pytest.mark.parametrize("year,days", [(2024, 366), (2023, 365)])
def test_days_in_year(year, days):
    assert days_in_year(year) == days


@pytest.mark.parametrize("iso3,name", [("BOL", "Bolivia"), ("IND", "India")])
def test_get_country_name(iso3, name):
    assert get_country_name(iso3) == name


def test_use_range():
    with pytest.raises(ValueError):
        use_range(20, 5, 15, "something beyond range")


@pytest.fixture
def sample_dataframe():
    """Fixture to create a sample DataFrame for testing."""
    return pd.DataFrame({
        'admin_level_0': ['Country1'],
        'admin_level_1': ['State1'],
        'admin_level_2': ['City1'],
        'admin_level_3': ['District1'],
        'metric': [100]
    })

@pytest.fixture
def existing_dataframe():
    """Fixture to create a mock existing DataFrame for testing."""
    return pd.DataFrame({
        'admin_level_0': ['Country1'],
        'admin_level_1': ['State1'],
        'admin_level_2': ['City1'],
        'admin_level_3': ['District1'],
        'metric': [50]  # Older value
    })

@patch('pandas.read_csv')
@patch('pandas.DataFrame.to_csv')
def test_update_or_create_output_create_new(mock_to_csv, mock_read_csv, sample_dataframe):
    """Test case when the CSV file does not exist, creating a new one."""
    # Simulate the file not existing
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = False

    # Call the function
    update_or_create_output(sample_dataframe, mock_path)

    # Check that read_csv was never called, and to_csv was called with the correct DataFrame
    mock_read_csv.assert_not_called()
    mock_to_csv.assert_called_once_with(mock_path, index=False)

@patch('pandas.read_csv')
@patch('pandas.DataFrame.to_csv')
def test_update_or_create_output_update_existing(mock_to_csv, mock_read_csv, sample_dataframe, existing_dataframe):
    """Test case when the CSV file already exists, updating it."""
    # Simulate the file existing
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True

    # Mock the read_csv call to return the existing dataframe
    mock_read_csv.return_value = existing_dataframe

    # Call the function
    update_or_create_output(sample_dataframe, mock_path)

    # Verify that read_csv was called with the correct path
    mock_read_csv.assert_called_once_with(mock_path, dtype={
        'admin_level_0': str, 'admin_level_1': str,
        'admin_level_2': str, 'admin_level_3': str
    })

    # Check that to_csv was called once with the correct merged DataFrame
    mock_to_csv.assert_called_once()
    saved_df = mock_to_csv.call_args[0][0]

    # Check that the merged DataFrame has the expected values
    assert saved_df.loc[0, 'metric'] == 100  # New value overrides the old one
    assert len(saved_df) == 1  # Should have only one row
