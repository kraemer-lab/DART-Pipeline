import pytest
from pathlib import Path

import numpy as np
from dart_pipeline.metrics.era5.collate import MetricCollection


@pytest.fixture(scope="module")
def metric_collection():
    return MetricCollection("VNM-2", data_path=Path("tests/data"))


def test_metric_collection(metric_collection):
    assert metric_collection.min_year == 2020
    assert metric_collection.max_year == 2020
    assert sorted(metric_collection.data.metric.unique()) == [
        "era5.2m_temperature.daily_max",
        "era5.2m_temperature.daily_mean",
        "era5.2m_temperature.daily_min",
        "era5.hydrological_balance.daily_sum",
        "era5.relative_humidity.daily_max",
        "era5.relative_humidity.daily_mean",
        "era5.relative_humidity.daily_min",
        "era5.specific_humidity.daily_max",
        "era5.specific_humidity.daily_mean",
        "era5.specific_humidity.daily_min",
        "era5.surface_solar_radiation_downwards.daily_sum",
        "era5.total_precipitation.daily_sum",
    ]


@pytest.mark.parametrize(
    "name,metric",
    [
        ("tp", "era5.total_precipitation.daily_sum"),
        ("mx2t", "era5.2m_temperature.daily_max"),
        ("t2m", "era5.2m_temperature.daily_mean"),
    ],
)
def test_collate_metric(name, metric, metric_collection):
    da = metric_collection.collate_metric(metric)
    assert da.name == name
    assert da.date.min() == np.datetime64("2020-01-06")
    assert da.date.max() == np.datetime64("2020-12-21")
    if "sum" in metric:
        assert "date: sum (interval: 1 week)" in da.attrs["cell_methods"]
    else:
        assert "date: mean (interval: 1 week)" in da.attrs["cell_methods"]


def test_collate_metric_errors(metric_collection):
    with pytest.raises(
        ValueError,
        match="No data found for metric='era5.2m_temperature.daily_median' from 2020-2020 in VNM-2",
    ):
        metric_collection.collate_metric("era5.2m_temperature.daily_median")
    with pytest.raises(
        ValueError,
        match="Contiguous years not present for metric='era5.2m_temperature.daily_mean' from 2019-2020 in VNM-2",
    ):
        metric_collection.collate_metric("era5.2m_temperature.daily_mean", (2019, 2020))


def test_collate(metric_collection):
    ds = metric_collection.collate()
    # fmt: off
    assert set(ds.data_vars) == {
        "mxr", "t2m", "r", "mnr", "q", "ssrd", "mx2t", "mn2t", "tp", "hb", "mxq", "mnq",
    }
    # fmt: on
