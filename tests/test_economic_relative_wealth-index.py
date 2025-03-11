"""Tests for functions in population-weighted/relative-wealth-index.py."""
from unittest import mock

from freezegun import freeze_time
import pandas as pd

from dart_pipeline.economic.relative_wealth_index import process_rwi


@freeze_time('2025-03-07')
@mock.patch('dart_pipeline.economic.relative_wealth_index.source_path')
@mock.patch('dart_pipeline.economic.relative_wealth_index.get_country_name')
@mock.patch('matplotlib.pyplot.savefig')
def test_process_rwi(mock_savefig, mock_get_country, mock_source_path):
    mock_source_path.return_value = 'mock_rwi.csv'
    mock_get_country.return_value = 'Mockland'

    # Mock RWI data
    rwi = pd.DataFrame({
        'latitude': [0.5, 1.5, 2.5],
        'longitude': [0.5, 1.5, 2.5],
        'rwi': [0.2, -0.3, 0.5]
    })

    with mock.patch('pandas.read_csv', return_value=rwi):
        output_df, filename = process_rwi('MLD', plots=False)

    expected_filename = 'MLD_economic_relative-wealth-index_2025-03-07.csv'
    assert filename == expected_filename
    assert 'GID_0' in output_df.columns
    assert 'value' in output_df.columns
    assert 'unit' in output_df.columns
    assert output_df.iloc[0]['GID_0'] == 'MLD'
    assert output_df.iloc[0]['value'] == rwi['rwi'].mean()
    assert not mock_savefig.called
