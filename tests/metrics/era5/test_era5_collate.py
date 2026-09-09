from pathlib import Path

import numpy as np
import pytest

from dart_pipeline.metrics.era5.collate import MetricCollection


@pytest.fixture(scope="module")
def metric_collection():
    return MetricCollection("HCM-2", data_path=Path("tests/data"), weekly=False)


def test_metric_collection(metric_collection):
    assert metric_collection.min_year == 2020
    assert metric_collection.max_year == 2021
    assert sorted(metric_collection.data.metric.unique()) == [
        "era5.core_daily",
    ]


@pytest.mark.parametrize(
    "name,metric,cell_methods",
    [
        ("tp", "era5.core_daily", "time: sum (interval: 1 day)"),
        (
            "mx2t24",
            "era5.core_daily",
            "time: max (interval: 1 day)",
        ),
        (
            "t2m",
            "era5.core_daily",
            "time: mean (interval: 1 day)",
        ),
    ],
)
def test_collate_metric(name, metric, cell_methods, metric_collection):
    da = metric_collection.collate_metric(metric)
    assert name in da.data_vars
    assert da.time.min() == np.datetime64("2020-01-01")
    assert da.time.max() == np.datetime64("2021-12-31")
    assert da[name].attrs["cell_methods"] == cell_methods


def test_collate_metric_errors(metric_collection):
    with pytest.raises(
        ValueError,
        match="No data found for metric='era5.2m_temperature.daily_median' from 2020-2021 in HCM-2",
    ):
        metric_collection.collate_metric("era5.2m_temperature.daily_median")
    with pytest.raises(
        ValueError,
        match="Contiguous years not present for metric='era5.core_daily' from 2019-2020 in HCM-2",
    ):
        metric_collection.collate_metric("era5.core_daily", (2019, 2020))


def test_collate(metric_collection):
    ds = metric_collection.collate()
    # fmt: off
    assert set(ds.data_vars).issubset({
        "mxr24", "t2m", "r", "mnr24", "q", "ssrd", "mx2t24", "mn2t24", "tp", "hb", "mxq24", "mnq24", "e"
    })
    # fmt: on

# Keep test on former API to ensure backward compatible
@pytest.fixture(scope="module")
def old_metric_collection():
    return MetricCollection("VNM-2", data_path=Path("tests/data"), weekly=False)


def test_old_metric_collection(old_metric_collection):
    assert old_metric_collection.min_year == 2020
    assert old_metric_collection.max_year == 2020
    assert sorted(old_metric_collection.data.metric.unique()) == [
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
    "name,metric,cell_methods",
    [
        ("tp", "era5.total_precipitation.daily_sum", "time: sum (interval: 1 day)"),
        (
            "mx2t",
            "era5.2m_temperature.daily_max",
            "time: max (interval: 1 day)",
        ),
        (
            "t2m",
            "era5.2m_temperature.daily_mean",
            "time: mean (interval: 1 day)",
        ),
    ],
)
def test_old_collate_metric(name, metric, cell_methods, old_metric_collection):
    da = old_metric_collection.collate_metric(metric)
    assert name in da.data_vars
    assert da.time.min() == np.datetime64("2020-01-01")
    assert da.time.max() == np.datetime64("2020-12-31")
    assert da[name].attrs["cell_methods"] == cell_methods


def test_old_collate_metric_errors(old_metric_collection):
    with pytest.raises(
        ValueError,
        match="No data found for metric='era5.2m_temperature.daily_median' from 2020-2020 in VNM-2",
    ):
        old_metric_collection.collate_metric("era5.2m_temperature.daily_median")
    with pytest.raises(
        ValueError,
        match="Contiguous years not present for metric='era5.2m_temperature.daily_mean' from 2019-2020 in VNM-2",
    ):
        old_metric_collection.collate_metric("era5.2m_temperature.daily_mean", (2019, 2020))


def test_old_collate(old_metric_collection):
    ds = old_metric_collection.collate()
    # fmt: off
    assert set(ds.data_vars).issubset({
        "mxr", "t2m", "r", "mnr", "q", "ssrd", "mx2t", "mn2t", "tp", "hb", "mxq", "mnq",
    })
    # fmt: on