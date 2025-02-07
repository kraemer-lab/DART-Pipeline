"""Tests for process functions in meteorological/era5reanalysis.py."""
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

from dart_pipeline.process import process_era5reanalysis


@pytest.fixture
def mock_nc_dataset():
    """Mock a NetCDF dataset."""
    mock_dataset = MagicMock()
    mock_variable = MagicMock()

    # Mock data
    mock_data = np.array([1.0, 2.0, 3.0])
    mock_variable.__getitem__.return_value = mock_data  # Simulate slicing
    mock_variable.long_name = 'Test Metric'
    mock_variable.units = 'Test Unit'
    mock_dataset.variables = {'test_variable': mock_variable}

    return mock_dataset


@patch('netCDF4.Dataset')
@patch('dart_pipeline.plots.plot_heatmap')
def test_process_era5reanalysis(
    mock_plot_heatmap, mock_nc_file, mock_nc_dataset
):
    """Test process_era5reanalysis function."""
    # Mock inputs
    dataset = 'test_dataset'
    partial_date = '2023-01-01'
    plots = False

    # Setup patches
    mock_nc_file.return_value = mock_nc_dataset
    mock_folder = MagicMock()
    mock_file = MagicMock()
    mock_file.name = f'{dataset}_{partial_date}.nc'
    mock_folder.iterdir.return_value = [mock_file]

    # Patch necessary components
    with patch('dart_pipeline.constants.BASE_DIR', Path('/mock/base/dir')), \
            patch(
                'dart_pipeline.constants.DEFAULT_SOURCES_ROOT',
                Path('sources')
            ), \
            patch(
                'dart_pipeline.constants.DEFAULT_OUTPUT_ROOT',
                Path('outputs')
            ), \
            patch('dart_pipeline.types.PartialDate.from_string') as \
            mock_partial_date:

        mock_partial_date.return_value.year = 2023
        mock_partial_date.return_value.month = 1
        mock_partial_date.return_value.day = 1
        mock_partial_date.return_value.__str__.return_value = partial_date

        with patch.object(Path, 'iterdir', return_value=[mock_file]):
            # Call the function
            df, output_file = process_era5reanalysis(
                dataset, partial_date, plots
            )

    # Validate output
    assert output_file == 'era5-reanalysis.csv'
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1

    # Validate data
    row = df.iloc[0]
    assert row['metric'] == 'Test Metric'
    assert row['value'] == 2.0  # Mean of [1.0, 2.0, 3.0]
    assert row['unit'] == 'Test Unit'
    assert row['year'] == 2023
    assert row['month'] == 1
    assert row['day'] == 1
