"""Test functions corresponding to dart_pipeline.metrics.era5"""

from pathlib import Path
from unittest.mock import patch

import xarray as xr

from dart_pipeline.metrics.era5 import prep_bias_correct


@patch("dart_pipeline.metrics.era5.util.get_path")
def test_prep_bias_correct(mock_get_path, singapore_region):
    mock_get_path.return_value = Path("tests/data")
    ds = prep_bias_correct(singapore_region, "2019-2021")
    assert set(ds.data_vars) == {"t2m", "r", "tp"}
    assert xr.infer_freq(ds.time) == "D"
    assert 0 <= ds.r.min().item() < ds.r.max().item() <= 100
