"""Tests for utility functions."""
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
def new_dataframe():
    """Fixture to create a sample data frame."""
    return pd.DataFrame({
        'iso3': ['VNM'],
        'admin_level_0': ['Vietnam'],
        'admin_level_1': ['An Giang'],
        'admin_level_2': ['An Phú'],
        'admin_level_3': ['Khánh An'],
        'year': [''],
        'month': [''],
        'day': [''],
        'week': [''],
        'metric': ['Example'],
        'value': [0.998],
        'unit': [''],
        'resolution': [''],
        'creation_date': [''],
    })


@pytest.fixture
def old_dataframe():
    """Fixture to create a mock existing data frame."""
    return pd.DataFrame({
        'iso3': ['VNM'],
        'admin_level_0': ['Vietnam'],
        'admin_level_1': ['An Giang'],
        'admin_level_2': ['An Phú'],
        'admin_level_3': ['Khánh An'],
        'year': [''],
        'month': [''],
        'day': [''],
        'week': [''],
        'metric': ['Example'],
        'value': [0.002],
        'unit': [''],
        'resolution': [''],
        'creation_date': [''],
    })


@patch('pandas.read_csv')
@patch('pandas.DataFrame.to_csv')
def test_update_or_create_output_create_new(
    mock_to_csv, mock_read_csv, new_dataframe
):
    """Test that when a file does not already exist a new one is created."""
    # Mock a non-existing file
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = False

    update_or_create_output(new_dataframe, mock_path)

    # Check that read_csv was never called
    mock_read_csv.assert_not_called()
    # Check that to_csv was called with the correct data frame
    mock_to_csv.assert_called_once_with(mock_path, index=False)


@patch('pandas.read_csv')
@patch('pandas.DataFrame.to_csv')
def test_update_or_create_output_update_existing(
    mock_to_csv, mock_read_csv, new_dataframe, old_dataframe
):
    """Test that when the file already exists it gets updated."""
    # Mock an existing file
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True

    # Mock the read_csv call to return the existing data frame
    mock_read_csv.return_value = old_dataframe

    # Call the function being tested
    df = update_or_create_output(new_dataframe, mock_path, return_df=True)

    # Verify that read_csv was called with the correct path
    mock_read_csv.assert_called_once_with(mock_path, dtype=str)

    # Check that to_csv was called once
    mock_to_csv.assert_called_once()
    # Check that the returned object is indeed a data frame
    assert isinstance(df, pd.DataFrame)
    # Check that the merged data frame has the expected values
    assert df.loc[0, 'value'].values[0] == '0.998'
    assert len(df) == 2


def test_update_or_create_output_invalid_input():
    """Test for when these is invalid input."""
    # Invalid data frame
    invalid_input = 'not_a_dataframe'
    mock_path = Path('dummy_path.csv')
    match = 'Expected a pandas DataFrame as input'
    with pytest.raises(TypeError, match=match):
        update_or_create_output(invalid_input, mock_path)

    # Invalid path
    df = pd.DataFrame({
        'iso3': ['AAA'],
        'admin_level_0': ['Country1'],
        'admin_level_1': ['State1'],
        'admin_level_2': ['City1'],
        'admin_level_3': ['District1'],
        'metric': [100]
    })
    invalid_path = 12345
    with pytest.raises(TypeError, match='Expected a valid file path'):
        update_or_create_output(df, invalid_path)
