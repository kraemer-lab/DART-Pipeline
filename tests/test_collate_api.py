"""
Tests for collate functions in test_collate_api.py
"""
from unittest.mock import patch, MagicMock

import pytest

from dart_pipeline.collate_api import download_era5_reanalysis_data


def test_download_era5_reanalysis_data():

    with patch('dart_pipeline.util.source_path') as mock_source_path, \
         patch('dart_pipeline.types.PartialDate.from_string') as mock_pdate, \
         patch('dart_pipeline.logging.info') as mock_logging:

        # Mocks for valid dataset
        mock_pdate.return_value = MagicMock(year=2023, month=10, day=1)
        mock_path = MagicMock()
        mock_path.parent.mkdir = MagicMock()
        mock_source_path.return_value = mock_path

        # Test valid dataset
        download_era5_reanalysis_data('reanalysis-era5-complete', '2023-10-01')

        # Assertions for valid dataset
        mock_pdate.assert_called_once_with('2023-10-01')
        mock_source_path.assert_called_once_with(
            'meteorological/era5-reanalysis',
            'reanalysis-era5-complete_2024-10-01.nc'
        )
        mock_path.parent.mkdir.assert_called_once_with(
            parents=True, exist_ok=True
        )
        mock_logging.assert_any_call(
            'dataset:%s', 'satellite-sea-ice-thickness'
        )
        mock_logging.assert_any_call('pdate:%s', mock_pdate.return_value)

        # Test invalid dataset
        with pytest.raises(ValueError):
            download_era5_reanalysis_data('reanalysis-era5-complete', '2023')

        # Test missing day for derived dataset
        mock_pdate.reset_mock()
        mock_pdate.return_value = MagicMock(year=2024, month=10, day=None)
        with pytest.raises(ValueError):
            download_era5_reanalysis_data(
                'reanalysis-era5-complete', '2024-10'
            )
