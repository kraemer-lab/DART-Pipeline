"""
Tests for collate functions in test_collate_api.py
"""
from unittest.mock import patch, MagicMock
from pathlib import Path

import pytest

from dart_pipeline.collate_api import download_era5_reanalysis_data
from dart_pipeline.constants import BASE_DIR


@patch('dart_pipeline.collate_api.unpack_file')
@patch('cdsapi.Client')
def test_download_era5_reanalysis_data(mock_cds_client, mock_source_path):
    # Set up mocks
    expected_path = BASE_DIR / Path(
        'data/sources/meteorological/era5-reanalysis/' +
        'satellite-sea-ice-thickness_2021.nc'
    )
    mock_source_path.return_value = expected_path

    # Mock mkdir method for the parent directory
    mock_mkdir = MagicMock()

    with patch.object(Path, 'mkdir', mock_mkdir):
        mock_cds_instance = mock_cds_client.return_value
        mock_cds_instance.retrieve = MagicMock()

        # Define test cases
        test_cases = [
            {
                'dataset': 'satellite-sea-ice-thickness',
                'partial_date': '2021-01-01',
                'expected_params': {
                    'satellite': ['cryosat_2'],
                    'cdr_type': ['icdr'],
                    'variable': 'all',
                    'year': ['2021'],
                    'month': ['01', '02', '03', '04', '10', '11', '12'],
                    'version': '3_0',
                    'format': 'netcdf',
                },
                'should_raise': None
            },
            {
                'dataset': 'derived-era5-land-daily-statistics',
                'partial_date': '2021-01',  # Invalid date (missing day)
                'expected_params': None,
                'should_raise': ValueError
            }
        ]

        for case in test_cases:
            dataset = case['dataset']
            partial_date = case['partial_date']

            if case['should_raise']:
                with pytest.raises(
                    case['should_raise'],
                    match='A day is required, eg "2024-10-01"'
                ):
                    download_era5_reanalysis_data(dataset, partial_date)
            else:
                # Call the function
                download_era5_reanalysis_data(dataset, partial_date)

                # Assertions
                expected_path = Path(
                    BASE_DIR, 'data/sources/meteorological/era5-reanalysis/' +
                    'satellite-sea-ice-thickness_2021.zip'
                )
                mock_cds_instance.retrieve.assert_called_with(
                    dataset, case['expected_params'], expected_path
                )
                mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

                # Ensure unpack_file was called
                mock_unpack_file.assert_called_once()

                # Reset mocks for the next iteration
                mock_cds_instance.retrieve.reset_mock()
                mock_mkdir.reset_mock()
                mock_unpack_file.reset_mock()
