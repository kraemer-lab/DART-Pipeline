"""Tests for functions in population-weighted/relative-wealth-index.py."""

from unittest import mock

import pandas as pd

from dart_pipeline.economic.relative_wealth_index import process_rwi


@mock.patch("dart_pipeline.economic.relative_wealth_index.get_path")
@mock.patch("dart_pipeline.economic.relative_wealth_index.get_country_name")
@mock.patch("matplotlib.pyplot.savefig")
def test_process_rwi(mock_savefig, mock_get_country, mock_get_path):
    mock_get_path.return_value = "mock_rwi.csv"
    mock_get_country.return_value = "Mockland"

    # Mock RWI data
    rwi = pd.DataFrame(
        {
            "latitude": [0.5, 1.5, 2.5],
            "longitude": [0.5, 1.5, 2.5],
            "rwi": [0.2, -0.3, 0.5],
        }
    )

    with mock.patch("pandas.read_csv", return_value=rwi):
        output_df = process_rwi("MLD", plots=False)

    assert "iso3" in output_df.columns
    assert "value" in output_df.columns
    assert "unit" in output_df.columns
    assert output_df.iloc[0]["iso3"] == "MLD"
    assert output_df.iloc[0]["value"] == rwi["rwi"].mean()
    assert not mock_savefig.called
