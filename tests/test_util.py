"""Tests for utility functions."""
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import requests_mock
import pandas as pd

from dart_pipeline.types import PartialDate
from dart_pipeline.util import download_file, days_in_year, get_country_name, \
    use_range, update_or_create_output, get_shapefile, \
    populate_output_df_admin_levels, populate_output_df_temporal


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
        'ISO3': ['VNM'],
        'COUNTRY': ['Vietnam'],
        'GID_1': ['VNM.1'],
        'NAME_1': ['An Giang'],
        'GID_2': ['VNM.1.1'],
        'NAME_2': ['An Phú'],
        'GID_3': ['VNM.1.1.1'],
        'NAME_3': ['Khánh An'],
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
        'ISO3': ['VNM'],
        'COUNTRY': ['Vietnam'],
        'GID_1': ['VNM.1'],
        'NAME_1': ['An Giang'],
        'GID_2': ['VNM.1.1'],
        'NAME_2': ['An Phú'],
        'GID_3': ['VNM.1.1.1'],
        'NAME_3': ['Khánh An'],
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
        'GID_0': ['AAA'],
        'COUNTRY': ['Country1'],
        'NAME_1': ['State1'],
        'NAME_2': ['City1'],
        'NAME_3': ['District1'],
        'metric': [100]
    })
    invalid_path = 12345
    with pytest.raises(TypeError, match='Expected a valid file path'):
        update_or_create_output(df, invalid_path)


@patch('dart_pipeline.util.source_path')
def test_get_shapefile(mock_source_path):
    iso3 = 'VNM'
    admin_level = '1'
    expected_path = Path('geospatial/gadm/VNM/gadm41_VNM_1.shp')

    mock_source_path.return_value = expected_path
    result = get_shapefile(iso3, admin_level)

    mock_source_path.assert_called_once_with(
        'geospatial/gadm', Path('VNM/gadm41_VNM_1.shp')
    )
    assert result == expected_path


def test_populate_output_df_admin_levels():
    base_df = pd.DataFrame({
        'GID_1': ['A'], 'NAME_1': ['B'],
        'GID_2': ['C'], 'NAME_2': ['D'],
        'GID_3': ['E'], 'NAME_3': ['F']
    })

    df_admin_0 = populate_output_df_admin_levels(base_df.copy(), '0')
    cols = ['GID_1', 'NAME_1', 'GID_2', 'NAME_2', 'GID_3', 'NAME_3']
    assert df_admin_0[cols].isnull().all().all()

    df_admin_1 = populate_output_df_admin_levels(base_df.copy(), '1')
    cols = ['GID_2', 'NAME_2', 'GID_3', 'NAME_3']
    assert df_admin_1[cols].isnull().all().all()
    assert df_admin_1[['GID_1', 'NAME_1']].notnull().all().all()

    df_admin_2 = populate_output_df_admin_levels(base_df.copy(), '2')
    assert df_admin_2[['GID_3', 'NAME_3']].isnull().all().all()
    cols = ['GID_1', 'NAME_1', 'GID_2', 'NAME_2']
    assert df_admin_2[cols].notnull().all().all()

    df_admin_3 = populate_output_df_admin_levels(base_df.copy(), '3')
    assert df_admin_3.notnull().all().all()


def test_populate_output_df_temporal():
    base_df = pd.DataFrame({'some_data': [1, 2, 3]})

    # Test with full date
    pdate_full = PartialDate(year=2023, month=5, day=15)
    df_full = populate_output_df_temporal(base_df.copy(), pdate_full)
    assert (df_full['year'] == 2023).all()
    assert (df_full['month'] == 5).all()
    assert (df_full['day'] == 15).all()
    assert df_full['week'].isnull().all()

    # Test with only year and month
    pdate_year_month = PartialDate(year=2023, month=5, day=None)
    df_year_month = populate_output_df_temporal(base_df.copy(), pdate_year_month)
    assert (df_year_month['year'] == 2023).all()
    assert (df_year_month['month'] == 5).all()
    assert df_year_month['day'].isnull().all()
    assert df_year_month['week'].isnull().all()

    # Test with only year
    pdate_year_only = PartialDate(year=2023, month=None, day=None)
    df_year_only = populate_output_df_temporal(base_df.copy(), pdate_year_only)
    assert (df_year_only['year'] == 2023).all()
    assert df_year_only['month'].isnull().all()
    assert df_year_only['day'].isnull().all()
    assert df_year_only['week'].isnull().all()
